 require 'rubygems'
 require 'dm-core'
 require 'dm-adapter-simpledb'
 
 #pass these into the script of set them in your environment using something like .bash_profile
 access_key  = ENV['AMAZON_ACCESS_KEY_ID']
 secret_key  = ENV['AMAZON_SECRET_ACCESS_KEY']

 # This is just a simple example script that shows you how to use the simpleDB DM adapter.
 # This example is just a single script not using any web frameworks.
 # This should illustrate basic usage of the adapter. Look at the specs for more example usage.
 # Obviously this code is not dry, or factored well, but it is intended to be a most simplistic example of usage.
 # If you have any questions please contact dan <at> mayerdan.com

 DOMAIN_FILE_MESSAGE = <<END
!!! ATTENTION !!!
In order to operate, these specs need a throwaway SimpleDB domain to operate
in. This domain WILL BE DELETED BEFORE EVERY SUITE IS RUN. In order to 
avoid unexpected data loss, you are required to manually configure the 
throwaway domain. In order to configure the domain, create a file in the
project root directory named THROW_AWAY_SDB_DOMAIN. It's contents should be 
the name of the SimpleDB domain to use for tests. E.g.

    $ echo dm_simpledb_adapter_test > THROW_AWAY_SDB_DOMAIN

END
 #fixes syntax highlighting ' 

 ROOT = File.expand_path('../', File.dirname(__FILE__))
 domain_file = File.join(ROOT, 'THROW_AWAY_SDB_DOMAIN')
 test_domain = if File.exist?(domain_file)
                 File.read(domain_file).strip
               else
                 warn DOMAIN_FILE_MESSAGE
                 exit 1
               end

 #configure and setup our datamapper connection to AWS simple DB
 DataMapper.setup(:default,
                  :adapter       => 'simpledb',
                  :access_key    => access_key,
                  :secret_key    => secret_key,
                  :domain        => test_domain
                  )

 #example datamapper resource
 class Tree
   include DataMapper::Resource
   
   property :id,   Serial
   property :name, String, :required=>true

   def to_s
     "name: #{name}"
   end
 end

 adapter = DataMapper::Repository.adapters[:default]
 sdb = adapter.sdb_interface

 sdb.delete_domain(test_domain) #delete domain if it exists
 sdb.create_domain(test_domain) #create domain so we can use it

 maple = Tree.new
 maple.name = "Acer rubrum"
 maple.save

 all_trees = Tree.all()
 puts "all trees" 
 puts all_trees 
 puts "simpleDB has eventual consistancy, meaning the object you put on it might not be there RIGHT away!"
 sleep(2) 
 all_trees = Tree.all()
 puts "all trees" 
 puts all_trees

 a_tree = Tree.first(:name => "Acer rubrum")
 puts "a tree"
 puts a_tree

 foo = Tree.create(:name => 'foo')
 a_tree = Tree.first(:name.like => "%foo%")
 puts "a tree"
 puts a_tree

 sdb.delete_domain(test_domain) #delete domain leaving it clean
