local cassandra = require "cassandra"

describe("Casandra", function()
  describe("consistencies", function()
    it("exposes Cassandra data consistencies", function()
      assert.is_table(cassandra.consistencies)

      local types = require "cassandra.types"
      for t in pairs(types.consistencies) do
        assert.truthy(cassandra.consistencies[t])
      end
    end)
  end)
  describe("cql_errors", function()
    it("exposes Cassandra CQL error types", function()
      assert.truthy(cassandra.cql_errors)

      local types = require "cassandra.types"
      for t in pairs(types.ERRORS) do
        assert.truthy(cassandra.cql_errors[t])
      end
    end)
  end)
  describe("auth", function()
    it("exposes default auth providers", function()
      assert.is_table(cassandra.auth)
      assert.truthy(cassandra.auth.PlainTextProvider)
    end)
  end)
  describe("shorthand serializers", function()
    it("require throws error on nil", function()
      assert.has_error(cassandra.uuid, "bad argument #1 to 'uuid' (got nil)")
      assert.has_error(cassandra.map, "bad argument #1 to 'map' (got nil)")
      assert.has_error(cassandra.list, "bad argument #1 to 'list' (got nil)")
      assert.has_error(cassandra.timestamp, "bad argument #1 to 'timestamp' (got nil)")
      local trace = debug.traceback()
      local match = string.find(trace, "stack traceback:\n\tspec/01-unit/cassandra_spec.lua", nil, true)
      assert.equal(1, match)
    end)
  end)
end)
