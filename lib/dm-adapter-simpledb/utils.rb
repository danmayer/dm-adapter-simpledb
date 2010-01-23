module DmAdapterSimpledb
  module Utils
    class NullObject
      def method_missing(*args, &block)
        self
      end
    end

    class NullSdbInterface
      def initialize(logger=NullObject.new)
        @logger = logger
      end

      def select(*args, &block)
        @logger.debug "[SELECT] #{args.inspect}"
        {
          :items => []
        }
      end

      def get_attributes(*args, &block)
        @logger.debug "[GET_ATTRIBUTES] #{args.inspect}"
        {}
      end

      def list_domains(*args, &block)
        @logger.debug "[LIST_DOMAINS] #{args.inspect}"
        {}
      end

      def put_attributes(*args, &block)
        @logger.debug "[PUT_ATTRIBUTES] #{args.inspect}"
        {}
      end

      def delete_attributes(*args, &block)
        @logger.debug "[DELETE_ATTRIBUTES] #{args.inspect}"
        {}
      end

      def create_domain(*args, &block)
        @logger.debug "[CREATE_DOMAIN] #{args.inspect}"
        {}
      end
    end

    def transform_hash(original, options={}, &block)
      original.inject({}){|result, (key,value)|
        value = if (options[:deep] && Hash === value) 
                  transform_hash(value, options, &block)
                else 
                  value
                end
        block.call(result,key,value)
        result
      }
    end
  end
end
