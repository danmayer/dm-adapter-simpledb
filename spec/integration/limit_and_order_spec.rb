require 'pathname'
require Pathname(__FILE__).dirname.expand_path + 'spec_helper'

class Hero
  include DataMapper::Resource
  
  property :id,         String, :key => true
  property :name,       String, :key => true
  property :age,        Integer
  property :wealth,     Float
  property :birthday,   Date
  property :created_at, DateTime
  
end

describe 'with multiple records saved' do
  before(:each) do
    @person_attrs = { :id => "person-#{Time.now.to_f.to_s}", :name => 'Jeremy Boles', :age  => 25, :wealth => 25.00, :birthday => Date.today }
    @jeremy   = Hero.create(@person_attrs.merge(:id => Time.now.to_f.to_s, :name => "Jeremy Boles", :age => 25))
    @danielle = Hero.create(@person_attrs.merge(:id => Time.now.to_f.to_s, :name => "Danille Boles", :age => 26))
    @keegan   = Hero.create(@person_attrs.merge(:id => Time.now.to_f.to_s, :name => "Keegan Jones", :age => 20, :wealth => 15.00))
    @adapter.wait_for_consistency
  end
  
  after(:each) do
    @jeremy.destroy
    @danielle.destroy
    @keegan.destroy
  end
  
  it 'should handle limit one case' do
    persons = Hero.all(:limit => 1)
    persons.length.should ==1
  end

  it 'should handle max item limit case' do
    persons = Hero.all(:limit => 3)
    persons.length.should ==3
  end

  it 'should handle max item if limit is large case' do
    persons = Hero.all(:limit => 150)
    persons.length.should ==3
  end

  it 'should handle ordering asc results with a limit' do
    persons = Hero.all(:order => [:age.asc], :limit => 2)
    persons.inspect #can't access via array until loaded? Weird
    persons.length.should ==2
    persons[0].should == @keegan
    persons[1].should == @jeremy
  end

  it 'should handle ordering asc results' do
    persons = Hero.all(:order => [:age.asc])
    persons.inspect #can't access via array until loaded? Weird
    persons[0].should == @keegan
    persons[1].should == @jeremy
    persons[2].should == @danielle
  end
  
  it 'should handle ordering desc results' do
    persons = Hero.all(:order => [:age.desc])
    persons.inspect #can't access via array until loaded? Weird
    persons[0].should == @danielle
    persons[1].should == @jeremy
    persons[2].should == @keegan
  end

  it 'should handle ordering results with multiple conditionss' do
    persons = Hero.all(:age.gt => 20, :wealth.gt => 20, :order => [:age.desc])
    persons.inspect #can't access via array until loaded? Weird
    persons.length.should ==2
    persons[0].should == @danielle
    persons[1].should == @jeremy
  end

  it 'should handle ordering results with ordered by conditions' do
    persons = Hero.all(:age.gt => 20, :order => [:age.desc])
    persons.inspect #can't access via array until loaded? Weird
    persons.length.should ==2
    persons[0].should == @danielle
    persons[1].should == @jeremy
  end

  it 'should handle ordering results with unorder by conditions' do
    persons = Hero.all(:wealth.gt => 20.00, :order => [:age.desc])
    persons.inspect #can't access via array until loaded? Weird
    persons.length.should ==2
    persons[0].should == @danielle
    persons[1].should == @jeremy
  end

  context "with many entries" do
    before :each do
      resources = []
      111.times do |i|
        resources << Hero.new(:id => i, :name => "Hero#{i}")
      end
      DataMapper.repository(:default).create(resources)
      @adapter.wait_for_consistency
    end

    it "should support limits over 100" do
      results = Hero.all(:limit => 110)
      results.should have(110).entries
    end

    it "should be able to page through results" do
      results1 = Hero.all(:limit => 10, :order => [:id.asc])
      results2 = Hero.all(:offset => 9, :limit => 10, :order => [:id.asc])
      results1.to_a.last.name.should be == results2.to_a.first.name
    end
  end

end
