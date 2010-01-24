module DmAdapterSimpledb
  class Table

    def self.name_from_metadata(metadata)
      Array(metadata).grep(/^table:(.*)$/) do |match|
        return $1
      end
      nil
    end

    def self.token_for(name)
      "table:#{name}"
    end

    attr_reader :model

    def initialize(model)
      @model = model
    end

    # Returns a string so we know what type of
    def simpledb_type
      model.storage_name(DataMapper.repository.name)
    end
    alias_method :storage_name, :simpledb_type

    def repository_name
      # TODO this should probably take into account the adapter
      model.repository.name
    end

    # Returns the keys for model sorted in alphabetical order
    def keys_for_model
      model.key(repository_name).sort {|a,b| a.name.to_s <=> b.name.to_s }
    end

    def token
      self.class.token_for(simpledb_type)
    end
  end
end
