require 'dm-types'

# NOTE: Do not try to clear SdbArray properties by assigning nil. Instead,
# assign an empty array:
#
#  resource.array_prop = []
#  resource.save
#
# The reason has to do with DataMapper's lazy-load handling - a lazy-loaded
# property has a value of nil until it is loaded. If you assign nil, DM thinks
# that 
module DataMapper
  module Types
    class SdbArray < DataMapper::Type
      primitive ::Object
      lazy      true

      def self.load(value, property)
        value
      end

      def self.dump(value, property)
        dumped = ::Object.new
        # This is a little screwy. DataMapper has a fixed list of values it
        # considers primitives, and it insists that the value that comes out of
        # a type's .dump() method MUST match one of these types. For SimpleDB
        # Array is effectively a primitive because of the way it stores values,
        # but DM doesn't include Array in it's list of valid primtive types. So
        # we need to return an object which IS considered a primitive - in this
        # case a plain 'ole Ruby Object. In order to convey the actual array
        # value to the backend, we tack on a #to_ary method which returns the
        # array data. Aws calls Array() on all values before writing them,
        # which in turn calls #to_ary(), and winds up with the correct data. In
        # effect we are sneaking the array data through DataMapper inside a
        # singleton method.
        singleton_class = (class << dumped; self; end)
        singleton_class.send(:define_method, :to_ary) do
          value
        end
        singleton_class.send(:define_method, :to_s) do
          value.to_s
        end
        dumped
      end

      def self.typecast(value, property)
        value
      end

    end 
  end
end
