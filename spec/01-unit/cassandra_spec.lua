local cassandra = require "cassandra"

describe("Casandra", function()
  describe("consistencies", function()
    it("should have Cassandra data consistency values available", function()
      assert.is_table(cassandra.consistencies)

      local types = require "cassandra.types"
      for t in pairs(types.consistencies) do
        assert.truthy(cassandra.consistencies[t])
      end
    end)
  end)
  describe("cql_errors", function()
    it("should have Cassandra CQL error types values available", function()
      assert.truthy(cassandra.cql_errors)

      local types = require "cassandra.types"
      for t in pairs(types.ERRORS) do
        assert.truthy(cassandra.cql_errors[t])
      end
    end)
  end)
  describe("shorthand serializers", function()
    it("should require the first argument (value)", function()
      assert.has_error(cassandra.uuid, "argument #1 required for 'uuid' type shorthand")
      assert.has_error(cassandra.map, "argument #1 required for 'map' type shorthand")
      assert.has_error(cassandra.list, "argument #1 required for 'list' type shorthand")
      assert.has_error(cassandra.timestamp, "argument #1 required for 'timestamp' type shorthand")
      local trace = debug.traceback()
      local match = string.find(trace, "stack traceback:\n\tspec/01-unit/cassandra_spec.lua", nil, true)
      assert.equal(1, match)
    end)
  end)
end)
