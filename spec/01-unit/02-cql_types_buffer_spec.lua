local helpers = require "spec.helpers"
local cassandra = require "cassandra"
local frame = require "cassandra.frame"
local Buffer = frame.buffer
local cql_types = frame.cql_types

for protocol_version = 2, 3 do

describe("CQL marshalling v"..protocol_version, function()
  for cql_t_name, fixtures in pairs(helpers.cql_fixtures) do
    local cql_t = cql_types[cql_t_name]
    local marshaller = cassandra[cql_t_name]

    it("["..cql_t_name.."]", function()
      for i = 1, #fixtures do
        local fixture = fixtures[i]
        local buf, decoded

        buf = Buffer.new(protocol_version)
        buf:write_cql_value(marshaller(fixture))
        buf:reset()
        decoded = buf:read_cql_value({__cql_type = cql_t})
        assert.fixture(cql_t_name, fixture, decoded)
      end
    end)
  end

  it("[list<T>]", function()
    local fixtures = helpers.cql_list_fixtures
    for i = 1, #fixtures do
      local fixture = fixtures[i]
      local buf, decoded

      buf = Buffer.new(protocol_version)
      buf:write_cql_value(fixture)
      buf:reset()
      decoded = buf:read_cql_value(fixture)
      assert.same(fixture.val, decoded)
    end
  end)

  it("[set<T>]", function()
    local fixtures = helpers.cql_set_fixtures
    for i = 1, #fixtures do
      local fixture = fixtures[i]
      local buf, decoded

      buf = Buffer.new(protocol_version)
      buf:write_cql_value(fixture)
      buf:reset()
      decoded = buf:read_cql_value(fixture)
      assert.same(fixture.val, decoded)
    end
  end)

  it("[map<T, T>]", function()
    local fixtures = helpers.cql_map_fixtures
    for i = 1, #fixtures do
      local fixture = fixtures[i]
      local buf, decoded

      buf = Buffer.new(protocol_version)
      buf:write_cql_value(fixture)
      buf:reset()
      decoded = buf:read_cql_value(fixture)
      assert.same(fixture.val, decoded)
    end
  end)

  it("[tuple<T, T>]", function()
    local fixtures = helpers.cql_tuple_fixtures
    for i = 1, #fixtures do
      local fixture = fixtures[i]
      local buf, decoded

      buf = Buffer.new(protocol_version)
      buf:write_cql_value(fixture)
      buf:reset()
      decoded = buf:read_cql_value(fixture)
      assert.same(fixture.val, decoded)
    end
  end)

  it("[udt]", function()
    local fixtures = helpers.cql_udt_fixtures
    for i = 1, #fixtures do
      local fixture = fixtures[i]
      local buf, decoded

      buf = Buffer.new(protocol_version)
      buf:write_cql_value(fixture)
      buf:reset()
      decoded = buf:read_cql_value(fixture)
      assert.same(fixture.read, decoded) -- read is different from write
    end
  end)

  describe("write_cql_values()", function()
    it("writes given values and infer their types", function()
      local values = {
        true,
        42,
        "hello world",
        {"hello", "world"},
        {hello = "world"}
      }

      local buf = Buffer.new(protocol_version)
      buf:write_short(#values)
      for i = 1, #values do
        buf:write_cql_value(values[i])
      end
      buf:reset()

      assert.equal(#values, buf:read_short())
      assert.True(buf:read_cql_value             {__cql_type = cql_types.boolean})
      assert.equal(values[2], buf:read_cql_value {__cql_type = cql_types.int})
      assert.equal(values[3], buf:read_cql_value {__cql_type = cql_types.text})
      assert.same(values[4], buf:read_cql_value  {__cql_type = cql_types.set,
                                                  __cql_type_value =
                                                    {__cql_type = cql_types.text}
                                                 })
      assert.same(values[5], buf:read_cql_value  {__cql_type = cql_types.map,
                                                  __cql_type_value = {
                                                    {__cql_type = cql_types.text},
                                                    {__cql_type = cql_types.text}
                                                  }
                                                 })
    end)
  end)
end)

end
