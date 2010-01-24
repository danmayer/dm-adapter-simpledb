require 'spec'
require 'spec/rake/spectask'
require 'pathname'

ROOT = Pathname(__FILE__).dirname.expand_path

task :default => [ 'spec:unit' ]

namespace :spec do
  desc 'Run unit-level specifications'
  Spec::Rake::SpecTask.new(:unit) do |t|
    if File.exists?('spec/spec.opts')
      t.spec_opts << '--options' << 'spec/spec.opts'
    end
    t.spec_files = Pathname.glob((ROOT + 'spec/unit/**/*_spec.rb').to_s)
    
    begin
      t.rcov = ENV.has_key?('NO_RCOV') ? ENV['NO_RCOV'] != 'true' : true
      t.rcov_opts << '--exclude' << 'spec'
      t.rcov_opts << '--text-summary'
      t.rcov_opts << '--sort' << 'coverage' << '--sort-reverse'
    rescue Exception
      # rcov not installed
    end
  end

  desc 'Run integration-level specifications'
  Spec::Rake::SpecTask.new(:integration) do |t|
    if File.exists?('spec/spec.opts')
      t.spec_opts << '--options' << 'spec/spec.opts'
    end
    t.spec_files = Pathname.glob((ROOT + 'spec/integration/**/*_spec.rb').to_s)
    
    begin
      t.rcov = ENV.has_key?('NO_RCOV') ? ENV['NO_RCOV'] != 'true' : true
      t.rcov_opts << '--exclude' << 'spec'
      t.rcov_opts << '--text-summary'
      t.rcov_opts << '--sort' << 'coverage' << '--sort-reverse'
    rescue Exception
      # rcov not installed
    end
  end

end

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name            = "dm-adapter-simpledb"
    gem.summary         = "DataMapper adapter for Amazon SimpleDB"
    gem.email           = "devs@devver.net"
    gem.homepage        = "http://github.com/devver/dm-adapter-simpledb"
    gem.description     = <<END
A DataMapper adapter for Amazon's SimpleDB service. 

Features:
 * Full set of CRUD operations
 * Supports all DataMapper query predicates.
 * Can translate many queries into efficient native SELECT operations.
 * Migrations
 * DataMapper identity map support for record caching
 * Lazy-loaded attributes
 * DataMapper Serial property support via UUIDs.
 * Array properties
 * Basic aggregation support (Model.count("..."))
 * String "chunking" permits attributes to exceed the 1024-byte limit

Note: as of version 1.0.0, this gem supports supports the DataMapper 0.10.*
series and breaks backwards compatibility with DataMapper 0.9.*.
END
    gem.authors         = [
      "Jeremy Boles",
      "Edward Ocampo-Gooding",
      "Dan Mayer",
      "Thomas Olausson",
      "Avdi Grimm"
    ]
    gem.add_dependency('dm-core',               '~> 0.10.0')
    gem.add_dependency('dm-aggregates',         '~> 0.10.0')
    gem.add_dependency('dm-migrations',         '~> 0.10.0')
    gem.add_dependency('dm-types',              '~> 0.10.0')
    gem.add_dependency('uuidtools',             '~> 2.0')
    gem.add_dependency('sdbtools',              '~> 0.4')
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler, or one of it's dependencies, is not available."
end
