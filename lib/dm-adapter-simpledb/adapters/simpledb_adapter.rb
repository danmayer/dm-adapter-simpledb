module DataMapper
  module Adapters
    class SimpleDBAdapter < AbstractAdapter
      include DmAdapterSimpledb::Utils

      attr_reader   :sdb_options
      attr_accessor :logger

      # For testing purposes ONLY. Seriously, don't enable this for production
      # code.
      attr_accessor :consistency_policy

      def initialize(name, normalised_options)
        super
        @sdb_options = {}
        @sdb_options[:access_key] = options.fetch(:access_key) { 
          options[:user] 
        }
        @sdb_options[:secret_key] = options.fetch(:secret_key) { 
          options[:password] 
        }
        @logger = options.fetch(:logger) { DataMapper.logger }
        @sdb_options[:logger] = @logger
        @sdb_options[:server] = options.fetch(:host) { 'sdb.amazonaws.com' }
        @sdb_options[:port]   = options[:port] || 443 # port may be set but nil
        @sdb_options[:domain] = options.fetch(:domain) { 
          options[:path].to_s.gsub(%r{(^/+)|(/+$)},"") # remove slashes
        }
        @sdb_options[:create_domain] = options.fetch(:create_domain) { false }
        # We do not expect to be saving any nils in future, because now we
        # represent null values by removing the attributes. The representation
        # here is chosen on the basis of it being unlikely to match any strings
        # found in real-world records, as well as being eye-catching in case any
        # nils DO manage to sneak in. It would be preferable if we could disable
        # RightAWS's nil-token replacement altogether, but that does not appear
        # to be an option.
        @sdb_options[:nil_representation] = "<[<[<NIL>]>]>"
        @null_mode = options.fetch(:null) { false }

        if @null_mode
          logger.info "SimpleDB adapter for domain #{domain_name} is in null mode"
        end

        @consistency_policy = 
          normalised_options.fetch(:wait_for_consistency) { false }
        @sdb = options.fetch(:sdb_interface) { nil }
        if @sdb_options[:create_domain] && !domains.include?(domain_name)
          @sdb_options[:logger].info "Creating domain #{domain_name}"
          database.create_domain(domain_name)
        end
      end

      def create(resources)
        created = 0
        transaction("CREATE") do
          resources.each do |resource|
            uuid = UUIDTools::UUID.timestamp_create
            initialize_serial(resource, uuid.to_i)

            record     = DmAdapterSimpledb::Record.from_resource(resource)
            attributes = record.writable_attributes
            item_name  = record.item_name
            domain.put(item_name, attributes)
            created += 1
          end
        end
        modified!
        created
      end
      
      def delete(collection)
        deleted = 0
        transaction("DELETE") do
          collection.each do |resource|
            record = DmAdapterSimpledb::Record.from_resource(resource)
            item_name = record.item_name
            domain.delete(item_name)
            deleted += 1
          end

          # TODO no reason we can't select a bunch of item names with an
          # arbitrary query and then delete them.
          raise NotImplementedError.new('Only :eql on delete at the moment') if not_eql_query?(collection.query)
        end
        modified!
        deleted
      end

      def read(query)
        maybe_wait_for_consistency
        transaction("READ") do |t|
          table = DmAdapterSimpledb::Table.new(query.model)

          query = query.dup
          query.update(extra_conditions(query))
          where_expression  = 
            DmAdapterSimpledb::WhereExpression.new(query.conditions, :logger => logger)
          selection_options = {
            :attributes => fields_to_request(query),
            :conditions => where_expression,
            :limit      => query_limit(query),
            :logger     => logger
          }
          selection_options.merge!(sort_instructions(query))
          selection = domain.selection(selection_options)
          selection.offset = query.offset unless query.offset.nil?

          records = selection.map{|name, attributes| 
            DmAdapterSimpledb::Record.from_simpledb_hash(name => attributes)
          }

          proto_resources = records.map{|record|
            record.to_resource_hash(query.fields)
          }
          
          query.clear
          query.update(:conditions => where_expression.unsupported_conditions)

          # This used to be a simple call to Query#filter_records(), but that
          # caused the result limit to be re-imposed on an already limited result
          # set, with the upshot that too few records were returned. So here we do
          # everything filter_records() does EXCEPT limiting.
          records = proto_resources
          records = records.uniq if query.unique?
          records = query.match_records(records)
          records = query.sort_records(records)

          records
        end
      end
      
      def update(attributes, collection)
        updated = 0
        transaction("UPDATE") do
          collection.each do |resource|
            updated_resource = resource.dup
            updated_resource.attributes = attributes
            record = DmAdapterSimpledb::Record.from_resource(updated_resource)
            attrs_to_update = record.writable_attributes
            attrs_to_delete = record.deletable_attributes
            item_name       = record.item_name
            unless attrs_to_update.empty?
              domain.put(item_name, attrs_to_update, :replace => true)
            end
            unless attrs_to_delete.empty?
              domain.delete(item_name, attrs_to_delete)
            end
            updated += 1
          end
          raise NotImplementedError.new('Only :eql on delete at the moment') if not_eql_query?(collection.query)
        end
        modified!
        updated
      end
      
      def query(query_call, query_limit = 999999999)
        SDBTools::Operation.new(sdb, :select, query_call).inject([]){
          |a, results| 
          a.concat(results[:items].map{|i| i.values.first})
        }[0...query_limit]
      end
      
      def aggregate(query)
        raise ArgumentError.new("Only count is supported") unless (query.fields.first.operator == :count)
        transaction("AGGREGATE") do |t|
          table    = DmAdapterSimpledb::Table.new(query.model)
          sdb_type = table.simpledb_type
          conditions, order, unsupported_conditions = set_conditions_and_sort_order(query, sdb_type)

          query_call = "SELECT count(*) FROM #{domain_name} "
          query_call << "WHERE #{conditions.compact.join(' AND ')}" if conditions.length > 0
          results = nil
          time = Benchmark.realtime do
            results = sdb.select(query_call)
          end; logger.debug(format_log_entry(query_call, time))
          [results[:items][0].values.first["Count"].first.to_i]
        end
      end

      # For testing purposes only.
      def wait_for_consistency
        return unless @current_consistency_token
        token = :none
        begin
          results = domain.get('__dm_consistency_token', '__dm_consistency_token')
          tokens  = Array(results[:attributes]['__dm_consistency_token'])
        end until tokens.include?(@current_consistency_token)
      end

      def domains
        database.domains
      end

    private
      def domain
        @domain ||= database.domain(@sdb_options[:domain])
      end

      # Returns the domain for the model
      def domain_name
        @sdb_options[:domain]
      end

      #sets the conditions and order for the SDB query
      def set_conditions_and_sort_order(query, sdb_type)
        unsupported_conditions = []
        conditions = ["simpledb_type = '#{sdb_type}'"]
        # look for query.order.first and insure in conditions
        # raise if order if greater than 1

        if query.order && query.order.length > 0
          query_object = query.order[0]
          #anything sorted on must be a condition for SDB
          conditions << "#{query_object.target.name} IS NOT NULL" 
          order = "ORDER BY #{query_object.target.name} #{query_object.operator}"
        else
          order = ""
        end
        query.conditions.each do |op|
          case op.slug
          when :regexp
            unsupported_conditions << op
          when :eql
            conditions << if op.value.nil?
              "#{op.subject.name} IS NULL"
            else
              "#{op.subject.name} = '#{op.value}'"
            end
          when :not then
            comp = op.operands.first
            if comp.slug == :like
              conditions << "#{comp.subject.name} not like '#{comp.value}'"
              next
            end
            case comp.value
            when Range, Set, Array, Regexp
              unsupported_conditions << op
            when nil
              conditions << "#{comp.subject.name} IS NOT NULL"
            else
              conditions << "#{comp.subject.name} != '#{comp.value}'"
            end
          when :gt then conditions << "#{op.subject.name} > '#{op.value}'"
          when :gte then conditions << "#{op.subject.name} >= '#{op.value}'"
          when :lt then conditions << "#{op.subject.name} < '#{op.value}'"
          when :lte then conditions << "#{op.subject.name} <= '#{op.value}'"
          when :like then conditions << "#{op.subject.name} like '#{op.value}'"
          when :in
            case op.value
            when Array, Set
              values = op.value.collect{|v| "'#{v}'"}.join(',')
              values = "'__NULL__'" if values.empty?                       
              conditions << "#{op.subject.name} IN (#{values})"
            when Range
              if op.value.exclude_end?
                unsupported_conditions << op
              else
                conditions << "#{op.subject.name} between '#{op.value.first}' and '#{op.value.last}'"
              end
            else
              raise ArgumentError, "Unsupported inclusion op: #{op.value.inspect}"
            end
          when :or
              # TODO There's no reason not to support OR
              unsupported_conditions << op
          else raise "Invalid query op: #{op.inspect}"
          end
        end
        [conditions,order,unsupported_conditions]
      end
      
      #gets all results or proper number of results depending on the :limit
      def get_results(query, conditions, order)
        fields_to_request = query.fields.map{|f| f.field}
        fields_to_request << DmAdapterSimpledb::Record::METADATA_KEY
        
        selection = SDBTools::Selection.new(
          sdb,
          domain_name,
          :attributes => fields_to_request)

        if query.order && query.order.length > 0
          query_object = query.order[0]
          #anything sorted on must be a condition for SDB
          conditions << "#{query_object.target.name} IS NOT NULL"
          selection.order_by = query_object.target.name
          selection.order    = case query_object.operator
                               when :asc then :ascending
                               when :desc then :descending
                               else raise "Unrecognized sort direction"
                               end
        end
        selection.conditions = conditions.compact.inject([]){|conds, cond|
          conds << " AND " unless conds.empty?
          conds << cond
        }
        if query.limit.nil?
          selection.limit = :none
        else
          selection.limit = query.limit
        end
        unless query.offset.nil?
          selection.offset = query.offset
        end

        items = []
        time = Benchmark.realtime do
          # TODO update Record to be created from name/attributes pair
          selection.each do |name, value| 
            items << {name => value}
          end
        end

        items
      end
      
      # Creates an item name for a query
      def item_name_for_query(query)
        sdb_type = simpledb_type(query.model)
        
        item_name = "#{sdb_type}+"
        keys = keys_for_model(query.model)
        conditions = query.conditions.sort {|a,b| a[1].name.to_s <=> b[1].name.to_s }
        item_name += conditions.map do |property|
          property[2].to_s
        end.join('-')
        Digest::SHA1.hexdigest(item_name)
      end
      
      def not_eql_query?(query)
        # Curosity check to make sure we are only dealing with a delete
        conditions = query.conditions.map {|c| c.slug }.uniq
        selectors = [ :gt, :gte, :lt, :lte, :not, :like, :in ]
        return (selectors - conditions).size != selectors.size
      end

      def database
        SDBTools::Database.new(
          @sdb_options[:access_key], 
          @sdb_options[:secret_key],
          :sdb_interface => sdb)
      end
      
      # Returns an SimpleDB instance to work with
      def sdb
        if @null_mode then return @sdb ||= NullSdbInterface.new(logger) end

        access_key = @sdb_options[:access_key]
        secret_key = @sdb_options[:secret_key]
        @sdb ||= RightAws::SdbInterface.new(access_key,secret_key,@sdb_options)
        @sdb
      end
      
      def update_consistency_token
        @current_consistency_token = UUIDTools::UUID.timestamp_create.to_s
        domain.put(
          '__dm_consistency_token', 
          {'__dm_consistency_token' => [@current_consistency_token]})
      end

      def maybe_wait_for_consistency
        if consistency_policy == :automatic && @current_consistency_token
          wait_for_consistency
        end
      end

      # SimpleDB supports "eventual consistency", which mean your data will be
      # there... eventually. Obviously this can make tests a little flaky. One
      # option is to just wait a fixed amount of time after every write, but
      # this can quickly add up to a lot of waiting. The strategy implemented
      # here is based on the theory that while consistency is only eventual,
      # chances are writes will at least be linear. That is, once the results of
      # write #2 show up we can probably assume that the results of write #1 are
      # in as well.
      #
      # When a consistency policy is enabled, the adapter writes a new unique
      # "consistency token" to the database after every write (i.e. every
      # create, update, or delete). If the policy is :manual, it only writes the
      # consistency token. If the policy is :automatic, writes will not return
      # until the token has been successfully read back.
      #
      # When waiting for the consistency token to show up, we use progressively
      # longer timeouts until finally giving up and raising an exception.
      def modified!
        case @consistency_policy
        when :manual, :automatic then
          update_consistency_token
        when false then
          # do nothing
        else
          raise "Invalid :wait_for_consistency option: #{@consistency_policy.inspect}"
        end
      end

      def transaction(description, &block)
        on_close = SDBTools::Transaction.log_transaction_close(logger)
        SDBTools::Transaction.open(description, on_close, &block)
      end

      def fields_to_request(query)
        fields = []
        fields.concat(query.fields.map{|f| f.field})
        fields.concat(DmAdapterSimpledb::Record::META_KEYS)
        fields.uniq!
        fields
      end

      def sort_instructions(query)
        direction = first_order_direction(query)
        if direction
          order_by = direction.target.field
          order    = case direction.operator
                     when :asc then :ascending
                     when :desc then :descending
                     else raise "Unrecognized sort direction"
                     end
          {:order_by => order_by, :order => order}
        else
          {}
        end
      end

      def extra_conditions(query)
        # SimpleDB requires all sort-by attributes to also be included in a
        # predicate.
        if (direction = first_order_direction(query))
          { direction.target.field.to_sym.not => nil }
        else
          {}
        end
      end
      
      def query_limit(query)
        query.limit.nil? ? :none : query.limit
      end

      def first_order_direction(query)
        Array(query.order).first
      end

    end # class SimpleDBAdapter

    
    # Required naming scheme.
    SimpledbAdapter = SimpleDBAdapter

    const_added(:SimpledbAdapter)

  end # module Adapters


end # module DataMapper

