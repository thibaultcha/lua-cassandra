local Marshall_v2 = require "cassandra.marshallers.marshall_v2"
local Unsmarshall_v2 = require "cassandra.marshallers.unmarshall_v2"

local marshall_v2 = Marshall_v2()
local unsmarshall_v2 = Unsmarshall_v2()

describe("Marshallers v2", function()

  local fixtures = {
    -- custom
    ascii = {"ascii"},
    bigint = {0, 42, -42, 42000000000, -42000000000},
    blob = {"\005\042", string.rep("blob", 10000)},
    boolean = {true, false},
    --counter
    -- decimal
    double = {0, 1.0000000000000004, -1.0000000000000004},
    float = {0, 3.14151, -3.14151},
    int = {0, 4200, -42},
    text = {"some text"},
    timestamp = {1405356926},
    uuid = {"1144bada-852c-11e3-89fb-e0b9a54a6d11"},
    varchar = {"string"},
    varint = {0, 4200, -42},
    timeuuid = {"1144bada-852c-11e3-89fb-e0b9a54a6d11"},
    inet = {["127.0.0.1"] = "127.0.0.1",
            ["2001:0db8:85a3:0042:1000:8a2e:0370:7334"] = "2001:0db8:85a3:0042:1000:8a2e:0370:7334",
            ["2001:0db8:0000:0000:0000:0000:0000:0001"] = "2001:db8::1",
            ["2001:0db8:85a3:0000:0000:0000:0000:0010"] = "2001:db8:85a3::10",
            ["2001:0db8:85a3:0000:0000:0000:0000:0100"] = "2001:db8:85a3::100",
            ["0000:0000:0000:0000:0000:0000:0000:0001"] = "::1",
            ["0000:0000:0000:0000:0000:0000:0000:0000"] = "::"}
  }

  for fix_type, fix_values in pairs(fixtures) do
    it("should encode and decode a ["..fix_type.."]", function()
      for expected, fix_value in pairs(fix_values) do
        local encoded = marshall_v2:value_representation(fix_value, marshall_v2.TYPES[fix_type])
        local buffer = unsmarshall_v2:create_buffer(encoded)
        local decoded = unsmarshall_v2:read_value(buffer, { id = marshall_v2.TYPES[fix_type] })

        if fix_type == "float" then
          local delta = 0.0000001
          assert.True(math.abs(decoded - fix_value) < delta)
        elseif fix_type == "inet" then
          assert.equal(expected, decoded)
        else
          assert.equal(fix_value, decoded)
        end
      end
    end)
  end

  it("should encode and decode a [list]", function()
    local list_fixtures = {
      {value_type = "text", value = {"abc", "def"}},
      {value_type = "int", value = {0, 1, 2, 42, -42}},
    }

    for _, fixture in ipairs(list_fixtures) do
      local encoded = marshall_v2:value_representation(fixture.value, marshall_v2.TYPES.list)
      local buffer = unsmarshall_v2:create_buffer(encoded)

      local value_type = { id = marshall_v2.TYPES[fixture.value_type] }

      local decoded = unsmarshall_v2:read_value(buffer, {
        id = marshall_v2.TYPES.list,
        value = value_type
      })
      assert.same(fixture.value, decoded)
    end
  end)

  it("should encode and decode a [map]", function()
    local map_fixtures = {
      {key_type = "text", value_type = "text", value = {k1='v1', k2='v2'}},
      {key_type = "text", value_type = "int", value = {k1=1, k2=2}},
      {key_type = "text", value_type = "int", value = {}},
    }

    for _, fixture in ipairs(map_fixtures) do
      local encoded = marshall_v2:value_representation(fixture.value, marshall_v2.TYPES.map)
      local buffer = unsmarshall_v2:create_buffer(encoded)

      local key_type = {id = marshall_v2.TYPES[fixture.key_type]}
      local value_type = {id = marshall_v2.TYPES[fixture.value_type]}

      local decoded = unsmarshall_v2:read_value(buffer, {
        id = marshall_v2.TYPES.map,
        value = {key_type, value_type}
      })
      assert.same(fixture.value, decoded)
    end
  end)

  it("should encode and decode a [set]", function()
    local set_fixtures = {
      {value_type = "text", value = {"abc", "def"}},
      {value_type = "int", value = {0, 1, 2, 42, -42}},
    }

    for _, fixture in ipairs(set_fixtures) do
      local encoded = marshall_v2:value_representation(fixture.value, marshall_v2.TYPES.set)
      local buffer = unsmarshall_v2:create_buffer(encoded)

      local value_type = {id = marshall_v2.TYPES[fixture.value_type]}

      local decoded = unsmarshall_v2:read_value(buffer, {
        id = marshall_v2.TYPES.set,
        value = value_type
      })
      assert.same(fixture.value, decoded)
    end
  end)

end)
