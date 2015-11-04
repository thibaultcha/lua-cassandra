local Buffer = require "cassandra.buffer"

describe("CQL Types", function()
  local FIXTURES = {
    boolean = {true, false},
    inet = {
      "127.0.0.1", "0.0.0.1", "8.8.8.8",
      "2001:0db8:85a3:0042:1000:8a2e:0370:7334",
      "2001:0db8:0000:0000:0000:0000:0000:0001"
    },
    int = {0, 4200, -42},
    set = {
      {"abc", "def"},
      {0, 1, 2, 42, -42}
    },
  }

  for fixture_type, fixture_values in pairs(FIXTURES) do
    it("["..fixture_type.."] should be bufferable", function()
      for _, fixture in ipairs(fixture_values) do
        local writer = Buffer(3)
        writer["write_cql_"..fixture_type](writer, fixture)
        local bytes = writer:dump()

        local reader = Buffer(3, bytes) -- protocol v3
        local decoded = reader["read_cql_"..fixture_type](reader)

        if type(fixture) == "table" then
          assert.same(fixture, decoded)
        else
          assert.equal(fixture, decoded)
        end
      end
    end)
  end
end)
