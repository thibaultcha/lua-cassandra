local Buffer = require "cassandra.buffer"

describe("Types", function()
  local FIXTURES = {
    short = {0, 1, -1, 12, 13},
    byte = {1, 2, 3},
    boolean = {true, false},
    integer = {0, 4200, -42},
    string = {"hello world"},
    string_map = {
      {hello = "world"},
      {cql_version = "3.0.0", foo = "bar"}
    }
  }

  for fixture_type, fixture_values in pairs(FIXTURES) do
    it("["..fixture_type.."] should be bufferable", function()
      for _, fixture in ipairs(fixture_values) do
        local writer = Buffer()
        writer["write_"..fixture_type](writer, fixture)
        local bytes = writer:write()

        local reader = Buffer(bytes)
        local decoded = reader["read_"..fixture_type](reader)

        if type(fixture) == "table" then
          assert.same(fixture, decoded)
        else
          assert.equal(fixture, decoded)
        end
      end
    end)
  end
end)
