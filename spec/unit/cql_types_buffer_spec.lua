local Buffer = require "cassandra.buffer"
local CONSTS = require "cassandra.consts"
local CQL_TYPES = require "cassandra.types.cql_types"

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
      local decoded = buf:read_cql_map({{id = fixture.key_type}, {id = fixture.value_type}})
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
      local decoded = buf:read_cql_set({id = fixture.value_type})
      assert.same(fixture.value, decoded)
    end
  end)
end)

end
