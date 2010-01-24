module DataMapper
  module Adapters
    class SimpleDBAdapter < AbstractAdapter
      include DmAdapterSimpledb::Utils

      attr_reader   :sdb_options
      attr_reader   :batch_limit
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
        @null_mode   = options.fetch(:null) { false }
        @batch_limit = options.fetch(:batch_limit) {
          SDBTools::Selection::DEFAULT_RESULT_LIMIT
        }.to_i

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
        transaction("CREATE #{resources.size} objects") do
          resources.each do |resource|
            uuid = UUIDTools::UUID.timestamp_create
            initialize_serial(resource, uuid.to_i)

            record     = DmAdapterSimpledb::Record.from_resource(resource)
            attributes = record.writable_attributes
            item_name  = record.item_name
            domain.put(item_name, attributes, :replace => true)
            created += 1
          end
        end
        modified!
        created
      end
      
      def delete(collection)
        deleted = 0
        transaction("DELETE #{collection.query.conditions}") do
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
        transaction("READ #{query.model.name} #{query.conditions}") do |t|
          query = query.dup

          selection = selection_from_query(query)

          records = selection.map{|name, attributes| 
            DmAdapterSimpledb::Record.from_simpledb_hash(name => attributes)
          }

          proto_resources = records.map{|record|
            record.to_resource_hash(query.fields)
          }
          
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
        transaction("UPDATE #{collection.query} with #{attributes.inspect}") do
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
      
      def aggregate(query)
        raise NotImplementedError, "Only count is supported" unless (query.fields.first.operator == :count)
        transaction("AGGREGATE") do |t|
          [selection_from_query(query).count]
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
        options = sdb ? {:sdb_interface => sdb} : {}
        @database ||= SDBTools::Database.new(
          @sdb_options[:access_key], 
          @sdb_options[:secret_key],
          options)
      end
      
      # Returns an SimpleDB instance to work with
      def sdb
        @sdb ||= (@null_mode ? NullSdbInterface.new(logger) : nil)
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

      # WARNING This method updates +query+ as a side-effect
      def selection_from_query(query)
        query.update(extra_conditions(query))
        where_expression  = 
          DmAdapterSimpledb::WhereExpression.new(query.conditions, :logger => logger)
        selection_options = {
          :attributes => fields_to_request(query),
          :conditions => where_expression,
          :batch_limit => batch_limit,
          :limit      => query_limit(query),
          :logger     => logger
        }
        selection_options.merge!(sort_instructions(query))
        selection = domain.selection(selection_options)
        selection.offset = query.offset unless query.offset.nil?
        query.clear
        query.update(:conditions => where_expression.unsupported_conditions)
        selection
      end

      def transaction(description, &block)
        on_close = SDBTools::Transaction.log_transaction_close(logger)
        SDBTools::Transaction.open(description, on_close, &block)
      end

      def fields_to_request(query)
        fields = []
        fields.concat(query.fields.map{|f| 
            f.field if f.respond_to?(:field)
          }.compact)
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
        conditions = if (direction = first_order_direction(query))
                       { direction.target.field.to_sym.not => nil }
                     else
                       {}
                     end
        table      = DmAdapterSimpledb::Table.new(query.model)
        meta_key   = DmAdapterSimpledb::Record::METADATA_KEY

        # The simpledb_type key is deprecated
        old_table_key = DmAdapterSimpledb::Record::STORAGE_NAME_KEY

        quoted_table_key = SDBTools::Selection.quote_name(old_table_key)
        quoted_key = SDBTools::Selection.quote_name(meta_key)
        conditions.merge!(
          :conditions => [
            "( #{quoted_key} = ? OR #{quoted_table_key} = ? )", 
            table.token,
            table.simpledb_type
          ])
        conditions
      end

      def query_limit(query)
        query.limit.nil? ? :none : query.limit
      end

      # SimpleDB only supports a single sort-by field. Further sorting has to be
      # handled locally.
      def first_order_direction(query)
        Array(query.order).first
      end


    end # class SimpleDBAdapter

    
    # Required naming scheme.
    SimpledbAdapter = SimpleDBAdapter

    const_added(:SimpledbAdapter)

  end # module Adapters


end # module DataMapper

