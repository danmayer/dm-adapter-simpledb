require 'spec'
ROOT = File.expand_path('../..', File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(ROOT,'lib'))
require 'right_aws'
require 'simpledb_adapter'

Spec::Runner.configure do |config|
  config.before :each do
    @sdb = stub("RightAWS::SdbInterface").as_null_object
    @log = stub("Log").as_null_object

    # Using Abstract adapter as a null DB
    DataMapper.setup(:default, 
      :adapter       => 'simpledb',
      :access_key    => "ACCESS_KEY",
      :secret_key    => "SECRET_KEY",
      :domain        => "DOMAIN",
      :logger        => @log,
      :sdb_interface => @sdb
      )
  end

  config.after :each do
    DataMapper::Repository.adapters.delete(:default)
  end
  
end
