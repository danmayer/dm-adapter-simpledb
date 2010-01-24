require 'dm-core'
require 'dm-adapter-simpledb/utils'
require 'dm-adapter-simpledb/chunked_string'
require 'dm-adapter-simpledb/table'

# TODO
# * V1.1: Store type in __dm_metadata
# * V1.1: Store type as non-munged class name

module DmAdapterSimpledb
  class Record
    include Utils

    METADATA_KEY     = "__dm_metadata"
    STORAGE_NAME_KEY = "simpledb_type"
    META_KEYS        = [METADATA_KEY, STORAGE_NAME_KEY]
    CURRENT_VERSION  = "01.01.00"

    def self.from_simpledb_hash(hash)
      data_version = data_version(simpledb_attributes(hash))
      versions.fetch(data_version) do
        raise "Unknown data version for: #{hash.inspect}"
      end.new(hash)
    end

    def self.from_resource(resource)
      versions.fetch(CURRENT_VERSION).new(resource)
    end

    def self.register(klass, version)
      versions[version] = klass
    end

    def self.versions
      @versions ||= {}
    end
    
    def self.version(version=nil)
      if version
        Record.register(self, version)
        @version = version
      else
        @version
      end
    end

    def self.data_version(simpledb_attributes)
      simpledb_attributes.fetch(METADATA_KEY){[]}.grep(/v\d\d\.\d\d\.\d\d/) do 
        |version_stamp|
        return version_stamp[1..-1]
      end
      return "00.00.00"
    end

    def self.simpledb_attributes(hash)
      hash.values.first
    end

    attr_reader :simpledb_attributes
    attr_reader :deletable_attributes
    attr_reader :item_name
    alias_method :writable_attributes, :simpledb_attributes

    def initialize(hash_or_resource)
      case hash_or_resource
      when DataMapper::Resource then
        attrs_to_update, attrs_to_delete = extract_attributes(hash_or_resource)
        @simpledb_attributes  = attrs_to_update
        @deletable_attributes = attrs_to_delete
        @item_name = item_name_for_resource(hash_or_resource)
        @model = hash_or_resource.model
      when Hash
        hash = hash_or_resource
        @item_name = hash.keys.first
        @simpledb_attributes  = hash.values.first
        @deletable_attributes = []
      else
        raise "Don't know how to initialize from #{hash_or_resource.inspect}"
      end
    end

    # Convert to a Hash suitable for initializing a Resource
    # 
    # @param [PropertySet] fields
    #   The fields to extract
    def to_resource_hash(fields)
      result = transform_hash(fields) {|hash, property|
        hash[property.name.to_s] = self[property.field, property]
      }
      result
    end

    # Deprecated - we are moving the type information under the metadata key
    def storage_name
      simpledb_attributes[STORAGE_NAME_KEY].first
    end

    def [](attribute, type)
      values = Array(simpledb_attributes[attribute])
      coerce_to(values, type)
    end

    def coerce_to(values, type_or_property)
      case type_or_property
      when DataMapper::Property
        coerce_to_property(values, type_or_property)
      when Class
        coerce_to_type(values, type_or_property)
      else raise "Should never get here"
      end
    end

    def coerce_to_property(value, property)
      property.typecast(coerce_to_type(value, property.type))
    end

    def coerce_to_type(values, type)
      case 
      when type <= String
        case values.size
        when 0
          nil
        when 1
          values.first
        else
          ChunkedString.new(values)
        end
      when type <= Array, type <= DataMapper::Types::SdbArray
        values
      else
        values.first
      end
    end

    def version
      self.class.version || self.class.data_version(simpledb_attributes)
    end

    def version_token
      "v#{version}"
    end

    # Returns the "Table" this record belongs to. SimpleDB has no concept of
    # tables, but we fake it with metadata.
    def table
      table_name_from_model || Table.name_from_metadata(metadata) || 
        storage_name
    end

    def table_name_from_model(repository=DataMapper.repository.name)
      if defined?(@model) && @model
        @model.storage_name(repository)
      end
    end

    def metadata
      simpledb_attributes[METADATA_KEY]
    end

    # Returns a record of the current version
    def migrate
      new_record = Record.versions[CURRENT_VERSION].allocate
      new_record.item_name = item_name
      data = transform_hash(simpledb_attributes) {
        |hash, key, values|
        hash[key] = coerce_heuristically(values)
      }
      updates = {}
      deletes = []
      data.each_pair do |key, values|
        if Array(values).empty?
          deletes << key
        else
          updates[key] = values
        end
      end
      new_record.add_metadata_to!(updates, table)
      new_record.simpledb_attributes = updates
      new_record.deletable_attributes = deletes
      new_record
    end

    def add_metadata_to!(hash, table_name)
      hash.merge!({
          STORAGE_NAME_KEY => [table_name],
          METADATA_KEY     => [version_token, Table.token_for(table_name)]
        })
    end

    protected

    attr_writer :item_name
    attr_writer :simpledb_attributes
    attr_writer :deletable_attributes

    private

    def app_data
      transform_hash(simpledb_attributes) {|h,k,v| 
        h[k] = v unless META_KEYS.include?(k)
      }
    end

    def extract_attributes(resource)
      attributes = resource.attributes(:property)
      attributes = attributes.to_a.map {|a| [a.first.name.to_s, a.last]}.to_hash
      attributes = adjust_to_sdb_attributes(attributes)
      updates, deletes = attributes.partition{|name,value|
        !Array(value).empty?
      }
      attrs_to_update = updates.inject({}){|h, (k,v)| h[k] = v; h}
      table = Table.new(resource.model)
      if resource.new?
        add_metadata_to!(attrs_to_update, table.simpledb_type)
      end
      attrs_to_delete = deletes.inject({}){|h, (k,v)| h[k] = v; h}.keys
      [attrs_to_update, attrs_to_delete]
    end

    # hack for converting and storing strings longer than 1024 one thing to
    # note if you use string longer than 1019 chars you will loose the ability
    # to do full text matching on queries as the string can be broken at any
    # place during chunking
    def adjust_to_sdb_attributes(attrs)
      attrs = transform_hash(attrs) do |result, key, value|
        if primitive_value_of(value.class) <= String
          result[key] = ChunkedString.new(value).to_a
        elsif value.class == Object # This is for SdbArray
          result[key] = value.to_ary
        elsif primitive_value_of(value.class) <= Array
          result[key] = value
        elsif value.nil?
          result[key] = nil
        else
          result[key] = [value.to_s]
        end
      end
      # Stringify keys
      transform_hash(attrs) {|h, k, v| h[k.to_s] = v}
    end

    def primitive_value_of(type)
      if type < DataMapper::Type
        type.primitive
      else
        type
      end
    end

    # Creates an item name for a resource
    def item_name_for_resource(resource)
      table = Table.new(resource.model)
      sdb_type = table.simpledb_type
      
      item_name = "#{sdb_type}+"
      keys = table.keys_for_model
      item_name += keys.map do |property|
        property.get(resource)
      end.join('-')
      
      Digest::SHA1.hexdigest(item_name)
    end

    def coerce_heuristically(values)
      if values
        case values.size
        when 0
          values
        when 1
          value = coerce_to_type(values, String)
          value.nil? ? [] : [value]
        else
          if ChunkedString.valid?(values)
            string = ChunkedString.new(values)
            coerced_string = coerce_to_type([string], Array).first
            ChunkedString.new(coerced_string).to_a
          else
            coerce_to_type(values, Array)
          end
        end
      else
        []
      end
    end

  end

  # Version 0 records are records that have no associated version
  # metadata. Any records created by versions of the DataMapper/SimplDB adapter
  # prior to 1.1.0 are considered to be version 0.
  #
  # Version 0 records have a few distinguishing characteristics:
  #  * An attribute with the token "nil" as its sole member is treated as a
  #    null/empty attribute.
  #  * The token "[[[NEWLINE]]]" inside of String attributes is replaced with \n
  class RecordV0 < Record
    version "00.00.00"

    def coerce_to_type(values, type)
      values = values.map{|v| replace_newline_placeholders(v)}
      result = super(values, type)

      if result == "nil"
        nil
      elsif result == ["nil"]
        []
      elsif result && type <= String
        # TODO redundant
        replace_newline_placeholders(result)
      else
        result
      end
    end

    private

    def replace_newline_placeholders(value)
      value && value.gsub("[[[NEWLINE]]]", "\n")
    end
  end

  class RecordV1_1 < Record
    version "01.01.00"
  end
end
