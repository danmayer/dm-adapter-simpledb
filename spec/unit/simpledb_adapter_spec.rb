require File.expand_path('unit_spec_helper', File.dirname(__FILE__))
require 'simpledb_adapter'

describe DataMapper::Adapters::SimpleDBAdapter do
  class Product
    include DataMapper::Resource

    property :id,    Serial
    property :name,  String
    property :stock, Integer
  end

  describe "given a record" do
    before :each do
      @record = Product.new(:name => "War and Peace", :stock => 3)
    end
    
    it "should be able to save the record" do
      @sdb.should_receive(:put_attributes).with(
        anything,
        anything,
        hash_including(
          'simpledb_type' => ["products"], 
          'stock'         => ["3"], 
          'name'          => ["War and Peace"]))
      @record.save
    end
  end

  describe "given an existing record" do
    before :each do
      @sdb.stub(:select).
        and_return(:items => [
          {"HANDLE" => {
              'id'    => ['12345'], 
              'name'  => ['War and Peace'], 
              'stock' => ['3']}}
        ])
      @record = Product.first
    end
    
    it "should be able to update the record" do
      @record.stock = 5
      @sdb.should_receive(:put_attributes).with(
        anything,
        anything,
        hash_including('stock' => ["5"]),
        :replace)
      @record.save
    end
  end

  describe "given a record exists in the DB" do
    before :each do
      @sdb.stub(:select).
        and_return(:items => [
          {"HANDLE" => {
              'id'    => ['12345'], 
              'name'  => ['War and Peace'], 
              'stock' => ['3'],
              '__dm_metadata' => ['v01.01.00', 'foobar']}}
        ])
    end
    
    it "should request metadata for the record" do
      @sdb.should_receive(:select).
        with(/select.*__dm_metadata.*from/i, anything).
        and_return(:items => [
          {"HANDLE" => {
              'id'    => ['12345'], 
              'name'  => ['War and Peace'], 
              'stock' => ['3'],
              '__dm_metadata' => ['v01.01.00', 'foobar']}}
        ])
      @record = Product.first
    end
  end

  # it "should be able to request items with an offset" do
  #   @sdb.should_receive(:select).
  #     with(/count(\*).*LIMIT 10000/, anything).
  #     exactly(1).times.
  #     ordered.
  #     and_return(:next_token => "TOKEN")
  #   @sdb.should_receive(:select).
  #     with(anything, "TOKEN").
  #     exactly(1).times.
  #     ordered.
  #     and_return(:items => [])
  #   @record = Product.all(:offset => 10000, :limit => 10)
  # end

end
