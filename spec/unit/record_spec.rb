require File.expand_path('unit_spec_helper', File.dirname(__FILE__))
require 'dm-adapter-simpledb/record'
require 'dm-adapter-simpledb/sdb_array'

describe DmAdapterSimpledb::Record do


  context "given a record from SimpleDB" do
    before :each do
      @thing_class = Class.new do
        include DataMapper::Resource

        property :foo, Integer
      end
      @it = DmAdapterSimpledb::Record.from_simpledb_hash(
        {"KEY" => {
            "foo"           => ["123"],
            "baz"           => ["456"],
            "simpledb_type" => ["thingies"]
          }
        })
    end

    it "should return nil when asked for a non-existant attribute as String" do
      @it["bar", String].should be_nil
    end

    it "should return nil when asked for a non-existant attribute as Integer" do
      @it["bar", Integer].should be_nil
    end

    it "should return [] when asked for a non-existant attribute as Array" do
      @it["bar", Array].should == []
    end

    it "should be able to coerce based on a property" do
      @it["foo", @thing_class.properties[:foo]].should == 123
    end

    it "should be able to coerce based on a simple type" do
      @it["foo", Integer].should == "123"
    end

    context "converted to a resource hash" do
      before :each do
        @hash = @it.to_resource_hash(@thing_class.properties)
      end

      it "should only include properties specified in the field set" do
        @hash.should_not include(:bar)
      end
    end

  end


  context "given a record with no version info" do
    before :each do
      @resource_class = Class.new do
        include DataMapper::Resource

        property :foo, Integer, :key => true
      end

      @it = DmAdapterSimpledb::Record.from_simpledb_hash(
        {"KEY" => {
            "foo"           => ["123"],
            "text"          => [
              "0001:line 1[[[NEWLINE]]]line 2",
              "0002:[[[NEWLINE]]]line 3[[[NEW",
              "0003:LINE]]]line 4"
            ],
            "short_text"    => ["foo[[[NEWLINE]]]bar"],
            "simpledb_type" => ["thingies"],
            "null_field"    => ["nil"],
            "array"         => ["foo[[[NEWLINE]]]bar", "baz"]
          }
        })
    end

    it "should identify the record as version 0" do
      @it.version.should == "00.00.00"
    end

    it "should be able to convert the record to a DM-friendly hash" do
      @it.to_resource_hash(@resource_class.properties).should == {
        "foo" => 123,
      }
    end

    it "should be able to extract the storage name" do
      @it.storage_name.should == "thingies"
    end

    it "should subtitute newlines for newline placeholders" do
      @it["text", String].should == 
        "line 1\nline 2\nline 3\nline 4"
    end

    it "should identify the table from the simpledb_type attributes" do
      @it.table.should == "thingies"
    end

    it "should not write any V1.1 metadata" do
      @it.writable_attributes.should_not include("__dm_metadata")
    end

    it "should interpret ['nil'] as the null value" do
      @it["null_field", String].should == nil
      @it["null_field", Array].should == []
    end

    describe "migrated to latest version" do
      before :each do
        @it = @it.migrate
      end

      it "should be the latest version" do
        @it.version.should == "01.01.00"
      end

      it "should mark nil attributes as deletable" do
        @it.writable_attributes.should_not include("null_field")
        @it.deletable_attributes.should include("null_field")
      end

      it "should contain valued attributes" do
        @it.writable_attributes["foo"].should == ["123"]
        @it.writable_attributes["text"].should ==
          ["line 1\nline 2\nline 3\nline 4"]
        @it.writable_attributes["short_text"].should == 
          ["foo\nbar"]
        @it.writable_attributes["array"].should ==
          ["foo\nbar", "baz"]
      end


      it "should have writable metadata attributes" do
        @it.writable_attributes["__dm_metadata"].should include("v01.01.00")
        @it.writable_attributes["__dm_metadata"].should include("table:thingies")
        @it.writable_attributes["simpledb_type"].should == ["thingies"]
      end
    end
  end

  context "given a version 1.1 record" do
    before :each do
      @resource_class = Class.new do
        include DataMapper::Resource
      
        property :bar, Integer
      end

      @it = DmAdapterSimpledb::Record.from_simpledb_hash(
        {"KEY" => {
            "__dm_metadata" => ["v01.01.00", "table:mystuff"],
            "bar"           => ["456"],
            "text"          => [
              "0001:line 1[[[NEWLINE]]]line 2",
              "0002:line 3[[[NEW",
              "0003:LINE]]]line 4"
            ],
          }
        })
    end

    it "should be a V1 record" do
      @it.should be_a_kind_of(DmAdapterSimpledb::RecordV1_1)
    end

    it "should identify the record as version 1" do
      @it.version.should == "01.01.00"
    end

    it "should be able to convert the record to a DM-friendly hash" do
      @it.to_resource_hash(@resource_class.properties).should == {
        "bar" => 456
      }
    end

    it "should not substitute newline tokens" do
      @it["text", String].should ==
        "line 1[[[NEWLINE]]]line 2line 3[[[NEWLINE]]]line 4"
    end

    it "should find the table given in the metadata" do
      @it.table.should == "mystuff"
    end
  end

  context "given a V1.1 record with a chunked string" do
    class Poem
      include ::DataMapper::Resource
      property :text, String
    end

    before :each do 
      @it = DmAdapterSimpledb::Record.from_simpledb_hash(
        {"KEY" => {
            "__dm_metadata" => ["v01.01.00"],
            "text" => [
              "0002:did gyre and gimbal in the wabe",
              "0001:twas brillig and the slithy toves\n",
            ]
          }
        })
    end

    it "should unchunk the text when asked to read it as a String" do
      @it["text",String].should == "twas brillig and the slithy toves\n" +
        "did gyre and gimbal in the wabe"
    end

    it "should return the chunks when asked to read it as an Array" do
      @it["text",Array].should == [
        "0002:did gyre and gimbal in the wabe",
        "0001:twas brillig and the slithy toves\n",
      ]
    end

    it "should return the first chunk when asked to read it as anything else" do
      @it["text", Integer].should == "0002:did gyre and gimbal in the wabe"
    end

    it "should be able to construct a resource hash" do
      @it.to_resource_hash(Poem.properties).should == {
        "text" => "twas brillig and the slithy toves\ndid gyre and gimbal in the wabe"
      }
    end

  end

  describe "given an unsaved (new) datamapper resource" do
    before :each do
      @resource_class = Class.new do
        include DataMapper::Resource
        storage_names[:default] = "books"
        storage_names[:backup]  = "tomes"

        property :author,       String, :key => true
        property :date,         Date
        property :text,         DataMapper::Types::Text
        property :tags,         DataMapper::Types::SdbArray
        property :isbn,         String
      end
      @text   = "lorem ipsum\n" * 100
      @date   = Date.new(2001,1,1)
      @author = "Cicero"
      @resource = @resource_class.new(
        :text => @text,
        :date => @date,
        :author => @author,
        :tags  => ['latin', 'classic'],
        :isbn  => nil)

      @it = DmAdapterSimpledb::Record.from_resource(@resource)
    end

    it "should be able to generate an item name" do
      @it.item_name.should == 
        Digest::SHA1.hexdigest("books+Cicero")
    end

    context "as a SimpleDB hash" do
      before :each do
        @hash = @it.writable_attributes
        @deletes = @it.deletable_attributes
      end

      it "should translate primitives successfully" do
        @hash["author"].should == ["Cicero"]
        @hash["date"].should   == ["2001-01-01"]
      end

      it "should chunk large text sections" do
        @hash["text"].should have(2).chunks
      end

      it "should be able to round-trip the text it chunks" do
        DmAdapterSimpledb::Record.from_simpledb_hash({"NAME" => @hash})["text", String].should ==
          @text
      end

      it "should translate arrays properly" do
        @hash["tags"].should == ['latin', 'classic']
      end

      it "should be able to round-trip arrays" do
        DmAdapterSimpledb::Record.from_simpledb_hash({"NAME" => @hash})["tags", DataMapper::Types::SdbArray].should ==
          ['latin', 'classic']
      end

      it "should not include nil values in writable attributes" do
        @hash.should_not include("isbn")
      end

      it "should include resource type in writable attributes" do
        @hash["simpledb_type"].should == ["books"]
      end

      it "should include nil values in deleteable attributes" do
        @deletes.should include("isbn")
      end

      it "should include version in writable attributes" do
        @hash["__dm_metadata"].should include("v01.01.00")
      end

      it "should include type in writable attributes" do
        @hash["__dm_metadata"].should include("table:books")
      end

    end

    it "should take repo into account when determining table name" do
      DataMapper.repository(:backup) do
        @it.table.should be == "tomes"
      end
    end

  end

  describe "given a saved datamapper resource" do
    before :each do
      @resource_class = Class.new do
        include DataMapper::Resource
        storage_names[:default] = "books"

        property :author,       String, :key => true
        property :date,         Date
        property :text,         DataMapper::Types::Text
        property :tags,         DataMapper::Types::SdbArray
        property :isbn,         String
      end
      @date   = Date.new(2001,1,1)
      @author = "Cicero"
      @resource = @resource_class.new(
        :text => "",
        :date => @date,
        :author => @author,
        :tags  => ['latin', 'classic'],
        :isbn  => nil)
      @resource.stub!(:saved? => true)
      @resource.stub!(:new? => false)

      @it = DmAdapterSimpledb::Record.from_resource(@resource)
    end

    it "should not include metadata in writable attributes" do
      @it.writable_attributes.should_not include("__dm_metadata")
    end
  end

end
