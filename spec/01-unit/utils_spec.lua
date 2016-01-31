local table_utils = require "cassandra.utils.table"

describe("table_utils", function()
  describe("extend_table", function()
    it("should extend a table from a source", function()
      local source = {source = true}
      local target = {target = true}

      target = table_utils.extend_table(source, target)
      assert.True(target.target)
      assert.True(target.source)
    end)
    it("should extend a table from multiple sources", function()
      local source1 = {source1 = true}
      local source2 = {source2 = true}
      local target = {target = true}

      target = table_utils.extend_table(source1, source2, target)
      assert.True(target.target)
      assert.True(target.source1)
      assert.True(target.source2)
    end)
    it("should extend nested properties", function()
      local source1 = {source1 = true, source1_nested = {hello = "world"}}
      local source2 = {source2 = true, source2_nested = {hello = "world"}}
      local target = {target = true}

      target = table_utils.extend_table(source1, source2, target)
      assert.True(target.target)
      assert.True(target.source1)
      assert.True(target.source2)
      assert.truthy(target.source1_nested)
      assert.truthy(target.source1_nested.hello)
      assert.equal("world", target.source1_nested.hello)
      assert.truthy(target.source2_nested)
      assert.truthy(target.source2_nested.hello)
      assert.equal("world", target.source2_nested.hello)
    end)
    it("should not override properties in the target", function()
      local source = {source = true}
      local target = {target = true, source = "source"}

      target = table_utils.extend_table(source, target)
      assert.True(target.target)
      assert.equal("source", target.source)
    end)
    it("should not override nested properties in the target", function()
      local source = {source = true, source_nested = {hello = "world"}}
      local target = {target = true, source_nested = {hello = "universe"}}

      target = table_utils.extend_table(source, target)
      assert.True(target.target)
      assert.truthy(target.source_nested)
      assert.truthy(target.source_nested.hello)
      assert.equal("universe", target.source_nested.hello)
    end)
    it("should not be mistaken by a `false` value", function()
      local source = {source = true}
      local target = {source = false}

      target = table_utils.extend_table(source, target)
      assert.False(target.source)
    end)
    it("should ignore targets that are not tables", function()
      local source = {foo = {bar = "foobar"}}
      local target = {foo = "hello"}

      assert.has_no_error(function()
        target = table_utils.extend_table(source, target)
      end)

      assert.equal("hello", target.foo)
    end)
  end)
end)
