module DmAdapterSimpledb
  class ChunkedString < String
    MAX_CHUNK_SIZE = 1019

    def self.valid?(values)
      values.all?{|v| v =~ /^\d{4}:/}
    end

    def initialize(string_or_array)
      case string_or_array
      when Array then super(chunks_to_string(string_or_array))
      else super(string_or_array)
      end
    end

    def to_ary
      string_to_chunks(self)
    end

    alias_method :to_a, :to_ary
    
    private

    def string_to_chunks(value)
      return [value] if value.size <= 1019
      chunks = value.to_s.scan(%r/.{1,1019}/m) # 1024 - '1024:'.size
      i = -1
      fmt = '%04d:'
      chunks.map!{|chunk| [(fmt % (i += 1)), chunk].join}
      raise ArgumentError, 'that is just too big yo!' if chunks.size >= 256
      chunks
    end
    
    def chunks_to_string(value)
      begin
        chunks =
          Array(value).flatten.map do |chunk|
          index, text = chunk.split(%r/:/, 2)
          [Float(index).to_i, text]
        end
        chunks.replace chunks.sort_by{|index, text| index}
        string_result = chunks.map!{|index, text| text}.join
        string_result
      rescue ArgumentError, TypeError
        #return original value, they could have put strings in the system not
        #using the adapter or previous versions that are larger than chunk size,
        #but less than 1024
        value.to_s
      end
    end

    
  end
end
