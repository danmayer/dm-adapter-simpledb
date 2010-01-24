require File.expand_path('../unit_spec_helper', File.dirname(__FILE__))
require 'dm-adapter-simpledb/where_expression'

module DmAdapterSimpledb
  include DataMapper::Query::Conditions

  describe WhereExpression do
    include DataMapper

    class Post
      include DataMapper::Resource
      
      property :id,    Serial
      property :title, String
      property :body,  Text
    end

    context "given a basic equality query" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:eql, Post.properties[:title], "FOO"))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title = "FOO"' }

      it "should have no unsupported conditions" do
        @it.unsupported_conditions.should be == Operation.new(:and)
      end
    end

    context "given a basic inequality query" do
      before :each do
        @conditions = Operation.new(
          :and,
          Operation.new(:not,
            Comparison.new(:eql, Post.properties[:title], "FOO")))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title != "FOO"' }
    end

    context "given a greater-than query" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:gt, Post.properties[:title], "FOO"))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title > "FOO"' }
    end

    context "given a lesser-than query" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:lt, Post.properties[:title], "FOO"))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title < "FOO"' }
    end

    context "given a equal-or-greater-than query" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:gte, Post.properties[:title], "FOO"))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title >= "FOO"' }
    end

    context "given an equal-to-or-lesser-than query" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:lte, Post.properties[:title], "FOO"))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title <= "FOO"' }
    end

    context "given a LIKE query" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:like, Post.properties[:title], "%FOO%"))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title LIKE "%FOO%"' }
    end

    context "given an IN query" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:in, Post.properties[:title], ["FOO", "BAZ"]))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title IN ("FOO", "BAZ")' }
    end

    context "given an IN query with an empty list" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:in, Post.properties[:title], []))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title IS NULL' }
    end

    context "given a negated IN query with an empty list" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:eql, Post.properties[:title], "FOO"),
          Operation.new(:not,
            Comparison.new(:in, Post.properties[:title], [])))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title = "FOO"' }
    end

    context "given an empty IN query OR another IN query" do
      before :each do
        @conditions = Operation.new(
          :or,
          Comparison.new(:in, Post.properties[:body], []),
          Comparison.new(:in, Post.properties[:title], ["foo"]))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'body IS NULL OR title IN ("foo")' }
    end

    context "given an IN query with a range" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:in, Post.properties[:title], ("A".."Z")))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title BETWEEN "A" AND "Z"' }
    end

    context "given an IN query with an exclusive range" do
      before :each do
        pending "Implementationm of exclusive ranges"
        @conditions = Operation.new(
          :and,
          Comparison.new(:in, Post.properties[:title], ("A"..."Z")),
          Comparison.new(:eql, Post.properties[:body], 42))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'body = "42" AND title BETWEEN "A" AND "Z"' }

      it "should include the range in unsupported conditions" do
        @it.unsupported_conditions.should be ==
          Operation.new(:and,
            Comparison.new(:in, Post.properties[:title], ("A"..."Z")))
      end
    end

    context "given a negated IN query with an exclusive range" do
      before :each do
        pending "Implementationm of exclusive ranges"
        @conditions = Operation.new(
          :and,
          Operation.new(:not,
            Comparison.new(:in, Post.properties[:title], (1...5))))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'NOT title BETWEEN "1" AND "5"' }

      it "should include the range in unsupported conditions" do
        @it.unsupported_conditions.should be ==
          Operation.new(:and,
            Operation.new(:not,
              Comparison.new(:in, Post.properties[:title], (1...5))))
        @it.unsupported_conditions.operands.first.operands.first.value.should be == (1...5)
      end
    end

    context "given a comparison to nil" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:eql, Post.properties[:title], nil))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title IS NULL' }
    end

    context "given a negated comparison to nil" do
      before :each do
        @conditions = Operation.new(
          :and,
          Operation.new(:not,
            Comparison.new(:eql, Post.properties[:title], nil)))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title IS NOT NULL' }
    end

    context "given a regexp comparison" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:regexp, Post.properties[:title], /foo/),
          Comparison.new(:eql, Post.properties[:body], "bar"))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'body = "bar"' }

      it "should include the regexp comparison in unsupported conds" do
        @it.unsupported_conditions.should be == 
          Operation.new(:and,
            Comparison.new(:regexp, Post.properties[:title], /foo/))
      end
    end

    context "given a literal expression with replacements" do
      before :each do
        @conditions = Operation.new(
          :and,
          ["body in (?, ?)", "FUZ", "BUZ"])
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'body in ("FUZ", "BUZ")' }
    end

    context "given a literal expression with subarray of replacements" do
      before :each do
        @conditions = Operation.new(
          :and,
          ["body in (?, ?)", ["FUZ", "BUZ"]])
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'body in ("FUZ", "BUZ")' }
    end

    context "given a literal expression" do
      before :each do
        @conditions = Operation.new(
          :and,
          ["body like '%frotz%'"])
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == "body like '%frotz%'" }
    end

    context "given a literal expression with a hash of replacements" do
      before :each do
        @conditions = Operation.new(
          :and,
          ["title = :title and body = :body", 
            {
              :title => "foo", :body => "bar"
            }
          ])
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'title = "foo" and body = "bar"' }
    end

    context "given a two ANDed comparisons" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:eql, Post.properties[:title], "FOO"),
          Comparison.new(:eql, Post.properties[:body],  "BAR"))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'body = "BAR" AND title = "FOO"' }
    end

    context "given an OR nested in an AND comparisons" do
      before :each do
        @conditions = Operation.new(
          :and,
          Comparison.new(:eql, Post.properties[:title], "FOO"),
          Operation.new(:or,
            Comparison.new(:eql, Post.properties[:body],  "BAR"),
            Comparison.new(:eql, Post.properties[:body],  "BAZ")))
        @it = WhereExpression.new(@conditions)
      end

      specify { 
        @it.to_s.should == '( body = "BAR" OR body = "BAZ" ) AND title = "FOO"'
      }
    end

    context "given a two ORed comparisons" do
      before :each do
        @conditions = Operation.new(
          :or,
          Comparison.new(:eql, Post.properties[:title], "FOO"),
          Comparison.new(:eql, Post.properties[:body],  "BAR"))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'body = "BAR" OR title = "FOO"' }
    end

    context "given an intersection" do
      before :each do
        @conditions = Operation.new(
          :or,
          Comparison.new(:eql, Post.properties[:title], "FOO"),
          Comparison.new(:eql, Post.properties[:body],  "BAR"))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'body = "BAR" OR title = "FOO"' }
    end

    context "given a negated AND comparison" do
      before :each do
        @conditions = Operation.new(
          :and,
          Operation.new(:not,
            Operation.new(:and,
              Comparison.new(:eql, Post.properties[:title], "FOO"),
              Comparison.new(:eql, Post.properties[:body],  "BAR"))))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'NOT ( body = "BAR" AND title = "FOO" )' }
    end

    context "given individually negated equality comparisons" do
      before :each do
        @conditions = Operation.new(
          :and,
          Operation.new(:not,
              Comparison.new(:eql, Post.properties[:title], "FOO")),
          Operation.new(:not,
              Comparison.new(:eql, Post.properties[:body],  "BAR")))
        @it = WhereExpression.new(@conditions)
      end

      specify { @it.to_s.should == 'body != "BAR" AND title != "FOO"' }
    end

  end
end
