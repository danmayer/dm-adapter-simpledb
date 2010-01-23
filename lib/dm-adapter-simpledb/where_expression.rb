module DmAdapterSimpledb
  class WhereExpression
    include DataMapper::Query::Conditions

    attr_reader   :conditions
    attr_accessor :logger

    def initialize(conditions, options={})
      @conditions = conditions
      @logger     = options.fetch(:logger){ DataMapper.logger }
    end

    def to_s
      node_to_expression(conditions)
    end

    def unsupported_conditions(node=conditions, top=true)
      case node
      when InclusionComparison
        if node.value.is_a?(Range) && node.value.exclude_end?
          logger.warn "Exclusive ranges are not supported natively by the SimpleDB adapter"
          node.dup
        end
      when RegexpComparison
        logger.warn "Regexp comparisons are not supported natively by the SimpleDB adapter"
        node.dup
      when AbstractOperation
        op_copy = node.dup
        op_copy.clear
        operands_copy = 
          node.operands.map{|o| unsupported_conditions(o,false)}.compact
        if operands_copy.size > 0
          op_copy.operands.merge(operands_copy)
          op_copy
        else
          top ? Operation.new(:and) : nil
        end
      else top ? Operation.new(:and) : nil
      end
    end

    private

    def node_to_expression(node)
      case node
      when AbstractOperation  then operation_to_expression(node)
      when AbstractComparison then comparison_to_expression(node)
      when Array then array_to_expression(node)
      when nil then nil
      else raise NotImplementedError, "Unrecognized node: #{node.inspect}"
      end
    end

    def operation_to_expression(operation)
      case operation
      when NotOperation
        operand = operation.sorted_operands.first # NOT only allowed to have 1 operand
        if operand.is_a?(EqualToComparison)
          if operand.value.nil?
            comparison_to_expression(operand, "IS NOT")
          else
            comparison_to_expression(operand, "!=")
          end
        else
          "NOT #{node_to_expression(operand)}"
        end
      when AndOperation then
        join_operands(operation, "AND")
      when OrOperation then
        join_operands(operation, "OR")
      when nil then nil
      else raise NotImplementedError, "Unhandled operation: #{operation.inspect}"
      end
    end

    def join_operands(operation, delimiter)
      operands = operation.sorted_operands
      joined_operands = Array(operands).map{|o| 
        catch(:unsupported) { node_to_expression(o) }
      }.compact.join(" #{delimiter} ")
      if operation.parent
        "( #{joined_operands} )"
      else
        joined_operands
      end
    end

    def comparison_to_expression(
        comparison, 
        operator=comparison_operator(comparison))
      field    = SDBTools::Selection.quote_name(comparison.subject.field)
      values   = value_to_expression(comparison.value)
      "#{field} #{operator} #{values}"
    end

    def array_to_expression(array)
      template, *replacements = *array.dup
      if replacements.size == 1 && replacements[0].is_a?(Hash)
        fill_template_from_hash(template, replacements[0])
      else
        fill_template_from_array(template, replacements)
      end
    end

    def fill_template_from_array(template, replacements)
      template.to_s.gsub("?") {|match| quote_value(replacements.shift.to_s) }
    end

    def fill_template_from_hash(template, replacements)
      template.to_s.gsub(/:\w+/) { |match|
        quote_value(replacements.fetch(match[1..-1].to_sym){match})
      }
    end

    def value_to_expression(value)
      case value
      when nil then "NULL"
      when Range then
        "#{quote_value(value.begin)} AND #{quote_value(value.end)}"
      when Array then
        value = value.map{|v| value_to_expression(v) }.join(", ")
        value = "(#{value})" if value.size > 1
      else quote_value(value)
      end
    end

    def quote_value(value)
      SDBTools::Selection.quote_value(value)
    end

    def comparison_operator(comparison)
      case comparison
      when EqualToComparison              then
        case comparison.value
        when nil then "IS"
        else "="
        end
      when GreaterThanComparison          then ">"
      when LessThanComparison             then "<"
      when GreaterThanOrEqualToComparison then ">="
      when LessThanOrEqualToComparison    then "<="
      when LikeComparison                 then "LIKE"
      when InclusionComparison            then
        case comparison.value
        when Range then "BETWEEN"
        else "IN"
        end
      when RegexpComparison then throw :unsupported
      else raise NotImplementedError, "Unhandled comparison: #{comparison.inspect}"
      end
    end
  end
end
