require 'pathname'
require Pathname(__FILE__).dirname.expand_path + 'spec_helper'

class Friend
  include DataMapper::Resource
  
  property :id,         String, :key => true
  property :name,       String, :key => true
  property :long_name,  String
  property :long_name_two,  String
  property :age,        Integer
  property :wealth,     Float
  property :birthday,   Date
  property :created_at, DateTime
  property :long_string, String
  
  belongs_to :network
end

class Network
  include DataMapper::Resource
  
  property :id,   String, :key => true
  property :name, String, :key => true
  
  has n, :friends
end

describe DataMapper::Adapters::SimpleDBAdapter do

  class Project
    include DataMapper::Resource
    property :id, Integer, :key => true
    property :project_repo, String
    property :repo_user, String
    property :description, String
  end


  LONG_VALUE =<<-EOF
    #!/bin/sh

set -o errexit

################################################################################
# build
#
# This hook is responsible for running a full "build" of the project for the 
# purpose of Continuus Integration
#
################################################################################

rake
# && rake metrics:all

#more
EOF

  before(:each) do


    @friend_attrs = { :id => "person-#{Time.now.to_f.to_s}", :name => 'Jeremy Boles', :age  => 25, :wealth => 25.00, :birthday => Date.today,
      :long_name => 'short', :long_name_two => 'short', :long_string => LONG_VALUE}
#.gsub("\n","br")
    @friend = Friend.new(@friend_attrs)
  end
  
  it 'should create a record' do
    @friend.save.should be_true
    @friend.id.should_not be_nil
    @friend.destroy
  end
  
  describe 'with a saved record' do
    before(:each) { @friend.save; sleep(0.4) } #sleep or it might not be on SDB at when the test checks it
    after(:each)  { @friend.destroy; sleep(0.4) } #same issues for the next test could still be there
    
    it 'should get a record' do
      person = Friend.get!(@friend.id, @friend.name)
      person.should_not be_nil
      person.wealth.should == @friend.wealth
    end
    
    it 'should not get records of the wrong type by id' do
      Network.get(@friend.id, @friend.name).should == nil
      lambda { Network.get!(@friend.id, @friend.name) }.should raise_error(DataMapper::ObjectNotFoundError)
    end    

    it 'should update a record' do
      person = Friend.get!(@friend.id, @friend.name)
      person.wealth = 100.00
      person.save
      sleep(0.3)
      person = Friend.get!(@friend.id, @friend.name)
      person.wealth.should_not == @friend.wealth
      person.age.should == @friend.age
      person.id.should == @friend.id
      person.name.should == @friend.name
    end

    it 'should update a record with a long string over 1024' do
      person = Friend.get!(@friend.id, @friend.name)
      long_string = "*" * 1026
      person.long_name = long_string
      person.save
      sleep(0.3)
      person = Friend.get!(@friend.id, @friend.name)
      person.long_name.should == long_string
      person.id.should == @friend.id
      person.name.should == @friend.name
    end

    it 'should update a record with with two long strings over 1024' do
      person = Friend.get!(@friend.id, @friend.name)
      long_string = "*" * 1026
      long_string_two = (0...2222).map{ ('a'..'z').to_a[rand(26)] }.join
      person.long_name = long_string
      person.long_name_two = long_string_two
      person.save
      sleep(0.3)
      person = Friend.get!(@friend.id, @friend.name)
      person.long_name.should == long_string
      person.long_name_two.should == long_string_two
      person.id.should == @friend.id
      person.name.should == @friend.name
    end

    it 'should save a record with string in the correct order' do
      person = Friend.get!(@friend.id, @friend.name)
      person.long_string.should == LONG_VALUE#.gsub("\n","br")
    end

    it 'should destroy a record' do
      @friend.destroy.should be_true
      sleep(0.4) #make sure SDB propigates change
      lambda {Friend.get!(@friend.id, @friend.name)}.should raise_error(DataMapper::ObjectNotFoundError)
      persons = Friend.all(:name => @friend.name)
      persons.length.should == 0
    end

    describe '#query' do
      before(:each) do
        @domain = Friend.repository(:default).adapter.sdb_options[:domain]
      end
      it "should return an array of records" do
        records = Friend.repository(:default).adapter.query("SELECT age, wealth from #{@domain} where age = '25'")
        records.should == [{"wealth"=>["25.0"], "age"=>["25"]}]
      end
      it "should return empty array if no matches" do
        records = Friend.repository(:default).adapter.query("SELECT age, wealth from #{@domain} where age = '15'")
        records.should be_empty
      end
      it "should raise an error if query is invalid" do
        lambda do
          records = Friend.repository(:default).adapter.query("SELECT gaga")
        end.should raise_error(RightAws::AwsError)
      end
    end
    describe 'aggregate' do
      it "should respond to count(*)" do
        Friend.count.should == 1
      end
      it "should not respond to any other aggregates" do
        lambda { Friend.min(:age) }.should raise_error(ArgumentError)
        lambda { Friend.max(:age) }.should raise_error(ArgumentError)
        lambda { Friend.avg(:age) }.should raise_error(ArgumentError)
        lambda { Friend.sum(:age) }.should raise_error(ArgumentError)
      end
    end
  end

  context "given a pre-existing v0 record" do
    before :each do
      @record_name = "33d9e5a6fcbd746dc40904a6766d4166e14305fe"
      record_attributes = {
        "simpledb_type"  => ["projects"], 
        "project_repo"   => ["git://github.com/TwP/servolux.git"], 
        "files_complete" => ["nil"], 
        "repo_user"      => ["nil"], 
        "id"             => ["1077338529"], 
        "description"    => [
            "0002:line 2[[[NEWLINE]]]line 3[[[NEW",
            "0001:line 1[[[NEWLINE]]]",
            "0003:LINE]]]line 4"
          ]
      }
      @sdb.put_attributes(@domain, @record_name, record_attributes)
      sleep 0.4
      @record = Project.get(1077338529)
    end

    it "should interpret legacy nil values correctly" do
      @record.repo_user.should be_nil
    end

    it "should interpret legacy strings correctly" do
      @record.description.should ==
        "line 1\nline 2\nline 3\nline 4"
    end

    it "should save legacy records without adding new metadata" do
      @record.repo_user = "steve"
      @record.save
      sleep 0.4
      attributes = @sdb.get_attributes(@domain, @record_name)[:attributes]
      attributes.should_not include("__dm_metadata")
    end
  end

  describe "given a brand-new record" do
    before :each do
      @record = Project.new(
        :repo_user    => "steve", 
        :id           => 123, 
        :project_repo => "git://example.org/foo")
    end

    it "should add metadata to the record on save" do
      @record.save
      sleep 0.4
      items = @sdb.select("select * from #{@domain} where id = '123'")[:items]
      attributes = items.first.values.first
      attributes["__dm_metadata"].should include("v01.01.00")
      attributes["__dm_metadata"].should include("table:projects")
    end
  end

end
