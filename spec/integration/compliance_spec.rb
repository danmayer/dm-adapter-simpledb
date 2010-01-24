require 'pathname'
require Pathname(__FILE__).dirname.expand_path + 'spec_helper'

require 'dm-core/spec/adapter_shared_spec'

describe DataMapper::Adapters::SimpleDBAdapter do
  before :all do
    @adapter = DataMapper::Repository.adapters[:default]
    @old_consistency_policy = @adapter.consistency_policy
    @adapter.consistency_policy = :automatic
    # @adapter.logger = ::Logger.new($stderr)
  end

  after :all do
    @adapter.consistency_policy = @old_consistency_policy
  end

  it_should_behave_like 'An Adapter'
end
