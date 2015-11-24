local table_utils = require "cassandra.utils.table"

describe("table_utils", function()
  describe("const_mt", function()

    local VERSION_CODES = {
      [2] = {
        REQUEST = 20,
        RESPONSE = 21,
        SOME_V2 = 2222
      },
      [3] = {
        REQUEST = 30,
        RESPONSE = 31,
        SOME_V3_ONLY = 3333
      }
    }
    setmetatable(VERSION_CODES, table_utils.const_mt)

    local FLAGS = {
      COMPRESSION = 1,
      TRACING = 2,
      [4] = {
        CUSTOM_PAYLOAD = 4
      }
    }
    setmetatable(FLAGS, table_utils.const_mt)

    describe("#get()", function()
      it("should get most recent version of a constant", function()
        assert.equal(30, VERSION_CODES:get("REQUEST"))
        assert.equal(31, VERSION_CODES:get("RESPONSE"))
        assert.equal(3333, VERSION_CODES:get("SOME_V3_ONLY"))
        assert.equal(2222, VERSION_CODES:get("SOME_V2"))
      end)
      it("should get constant from the root", function()
        assert.equal(1, FLAGS:get("COMPRESSION"))
        assert.equal(2, FLAGS:get("TRACING"))
      end)
      it("should accept a version parameter for which version to look into", function()
        assert.equal(4, FLAGS:get("CUSTOM_PAYLOAD", 4))
        assert.equal(20, VERSION_CODES:get("REQUEST", 2))
        assert.equal(21, VERSION_CODES:get("RESPONSE", 2))
      end)
    end)

  end)
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
  end)
end)
