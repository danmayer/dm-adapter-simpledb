module DataMapper
  module Migrations
     #integrated from http://github.com/edward/dm-simpledb/tree/master
      module SimpledbAdapter
        
        module ClassMethods

        end

        def self.included(other)
          other.extend ClassMethods

          DataMapper.extend(::DataMapper::Migrations::SingletonMethods)

          [ :Repository, :Model ].each do |name|
            ::DataMapper.const_get(name).send(:include, Migrations.const_get(name))
          end
        end

        # Returns whether the storage_name exists.
        # @param storage_name<String> a String defining the name of a domain
        # @return <Boolean> true if the storage exists
        def storage_exists?(storage_name)
          domains = sdb_interface.list_domains[:domains]
          domains.detect {|d| d == storage_name }!=nil
        end

        def create_model_storage(model)
          database.create_domain(@sdb_options[:domain])
        end
        
        #On SimpleDB you probably don't want to destroy the whole domain
        #if you are just adding fields it is automatically supported
        #default to non destructive migrate, to destroy run
        #rake db:automigrate destroy=true
        def destroy_model_storage(model)
          if ENV['destroy']!=nil && ENV['destroy']=='true'
            sdb_interface.delete_domain(@sdb_options[:domain])
          end
        end

      end # module Migration
  end # module Migration
end

