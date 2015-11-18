local cassandra = require "cassandra"
local Buffer = require "cassandra.buffer"
local CONSTS = require "cassandra.constants"
local types = require "cassandra.types"
local CQL_TYPES = types.cql_types

for _, protocol_version in ipairs(CONSTS.SUPPORTED_PROTOCOL_VERSIONS) do

describe("CQL Types protocol v"..protocol_version, function()
  local FIXTURES = {
    boolean = {true, false},
    inet = {
      "127.0.0.1", "0.0.0.1", "8.8.8.8",
      "2001:0db8:85a3:0042:1000:8a2e:0370:7334",
      "2001:0db8:0000:0000:0000:0000:0000:0001"
    },
    int = {0, 4200, -42},
    uuid = {"1144bada-852c-11e3-89fb-e0b9a54a6d11"}
  }

  for fixture_type, fixture_values in pairs(FIXTURES) do
    it("["..fixture_type.."] should be bufferable", function()
      for _, fixture in ipairs(fixture_values) do
        local buf = Buffer(protocol_version)
        buf["write_cql_"..fixture_type](buf, fixture)
        buf:reset()

        local decoded = buf["read_cql_"..fixture_type](buf)
        if type(fixture) == "table" then
          assert.same(fixture, decoded)
        else
          assert.equal(fixture, decoded)
        end
      end
    end)

    describe("manual type infering", function()
      it("should be possible to infer the type of a value through helper methods", function()
        for _, fixture in ipairs(fixture_values) do
          local infered_value = cassandra.types[fixture_type](fixture)
          local buf = Buffer(protocol_version)
          buf:write_cql_value(infered_value)
          buf:reset()

          local decoded = buf:read_cql_value({type_id = CQL_TYPES[fixture_type]})
          if type(fixture) == "table" then
            assert.same(fixture, decoded)
          else
            assert.equal(fixture, decoded)
          end
        end
      end)
    end)
  end

  it("[map<type, type>] should be bufferable", function()
    local MAP_FIXTURES = {
      {key_type = CQL_TYPES.text, value_type = CQL_TYPES.text, value = {k1 = "v1", k2 = "v2"}},
      {key_type = CQL_TYPES.text, value_type = CQL_TYPES.int, value = {k1 = 1, k2 = 2}},
      {key_type = CQL_TYPES.text, value_type = CQL_TYPES.int, value = {}}
    }

    for _, fixture in ipairs(MAP_FIXTURES) do
      local buf = Buffer(protocol_version)
      buf:write_cql_map(fixture.value)
      buf:reset()
      local decoded = buf:read_cql_map({{type_id = fixture.key_type}, {type_id = fixture.value_type}})
      assert.same(fixture.value, decoded)
    end
  end)

  it("[set<type>] should be bufferable", function()
    local SET_FIXTURES = {
      {value_type = CQL_TYPES.text, value = {"abc", "def"}},
      {value_type = CQL_TYPES.int, value = {1, 2 , 0, -42, 42}}
    }

    for _, fixture in ipairs(SET_FIXTURES) do
      local buf = Buffer(protocol_version)
      buf:write_cql_set(fixture.value)
      buf:reset()
      local decoded = buf:read_cql_set({type_id = fixture.value_type})
      assert.same(fixture.value, decoded)
    end
  end)

  describe("write_cql_values", function()
    it("should loop over given values and infer their types", function()
      local values = {
        42,
        {"hello", "world"},
        {hello = "world"},
        "hello world"
      }

      local buf = Buffer(protocol_version)
      buf:write_cql_values(values)
      buf:reset()
      assert.equal(#values, buf:read_short())
      assert.equal(values[1], buf:read_cql_int())
      assert.same(values[2], buf:read_cql_set({type_id = CQL_TYPES.text}))
      assert.same(values[3], buf:read_cql_map({{type_id = CQL_TYPES.text}, {type_id = CQL_TYPES.text}}))
      assert.same(values[4], buf:read_cql_raw())
    end)
  end)
end)

end

