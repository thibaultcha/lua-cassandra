local utils = require "spec.spec_utils"
local cassandra = require "cassandra"
local Buffer = require "cassandra.buffer"
local types = require "cassandra.types"
local CQL_TYPES = types.cql_types

local SUPPORTED_PROTOCOL_VERSIONS = {cassandra.DEFAULT_PROTOCOL_VERSION, cassandra.MIN_PROTOCOL_VERSION}

for _, protocol_version in ipairs(SUPPORTED_PROTOCOL_VERSIONS) do

describe("CQL Types protocol v"..protocol_version, function()
  it("[uuid] should be bufferable", function()
    local fixture = "1144bada-852c-11e3-89fb-e0b9a54a6d11"
    local buf = Buffer(protocol_version)
    buf:write_cql_uuid(fixture)
    buf:reset()
    local decoded = buf:read_cql_uuid()
    assert.equal(fixture, decoded)
  end)

  for fixture_type, fixture_values in pairs(utils.cql_fixtures) do
    it("["..fixture_type.."] should be bufferable", function()
      for _, fixture in ipairs(fixture_values) do
        local buf = Buffer(protocol_version)
        buf["write_cql_"..fixture_type](buf, fixture)
        buf:reset()

        local decoded = buf["read_cql_"..fixture_type](buf)
        assert.validFixture(fixture_type, fixture, decoded)
      end
    end)

    describe("manual type infering", function()
      it("["..fixture_type.."] should be possible to infer the type of a value through short-hand methods", function()
        for _, fixture in ipairs(fixture_values) do
          local infered_value = cassandra[fixture_type](fixture)
          local buf = Buffer(protocol_version)
          buf:write_cql_value(infered_value)
          buf:reset()

          local decoded = buf:read_cql_value({type_id = CQL_TYPES[fixture_type]})
          assert.validFixture(fixture_type, fixture, decoded)
        end
      end)
    end)
  end

  it("[list<type>] should be bufferable", function()
    for _, fixture in ipairs(utils.cql_list_fixtures) do
      local buf = Buffer(protocol_version)
      buf:write_cql_set(fixture.value)
      buf:reset()
      local decoded = buf:read_cql_list({type_id = fixture.value_type})
      assert.same(fixture.value, decoded)
    end
  end)

  it("[map<type, type>] should be bufferable", function()
    for _, fixture in ipairs(utils.cql_map_fixtures) do
      local buf = Buffer(protocol_version)
      buf:write_cql_map(fixture.value)
      buf:reset()
      local decoded = buf:read_cql_map({{type_id = fixture.key_type}, {type_id = fixture.value_type}})
      assert.same(fixture.value, decoded)
    end
  end)

  it("[set<type>] should be bufferable", function()
    for _, fixture in ipairs(utils.cql_set_fixtures) do
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

