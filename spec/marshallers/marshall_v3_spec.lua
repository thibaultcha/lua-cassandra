local marshall_v3 = require "cassandra.marshallers.marshall_v3"
local unsmarshall_v3 = require "cassandra.marshallers.unmarshall_v3"

describe("Marshallers v3", function()

  it("should encode and decode a [tuple]", function()
    local fixtures = {
      { types = {"text", "int", "float"}, values = {"foo", 1, 3.14151} },
      { types = {"text", "int"}, values = {"foo", 1} },
      { types = {"text", "text", "int"}, values = {"foo", "abcd", 123} }
    }

    for _, fixture in ipairs(fixtures) do
      local tuple_type = {
        id = marshall_v3.TYPES.tuple,
        fields = {}
      }
      for _, part_type in ipairs(fixture.types) do
        table.insert(tuple_type.fields, {type = { id = marshall_v3.TYPES[part_type] }})
      end

      local encoded = marshall_v3.value_representation(fixture.values, marshall_v3.TYPES.tuple)
      local buffer = unsmarshall_v3.create_buffer(encoded)
      local decoded = unsmarshall_v3.read_value(buffer, tuple_type)

      for i, v in ipairs(decoded) do
        if fixture.types[i] == "float" then
          local delta = 0.0000001
          assert.True(math.abs(v - fixture.values[i]) < delta)
        else
          assert.equal(fixture.values[i], v)
        end
      end
    end
  end)

  it("should encode and decode a [udt]", function()
    local fixtures = {
      { types = {some_text="text", some_int="int", some_float="float"}, values = {"foo", 1, 3.14151} },
      { types = {some_text="text", some_int="int"}, values = {"foo", 1} }
    }

    for _, fixture in ipairs(fixtures) do
      local udt_type = {
        id = marshall_v3.TYPES.udt,
        fields = {}
      }
      for part_name, part_type in pairs(fixture.types) do
        table.insert(udt_type.fields, { type = {id=marshall_v3.TYPES[part_type]},
                                        name = part_name })
      end

      local encoded = marshall_v3.value_representation(fixture.values, marshall_v3.TYPES.udt)
      local buffer = unsmarshall_v3.create_buffer(encoded)
      local decoded = unsmarshall_v3.read_value(buffer, udt_type)

      for i, v in ipairs(decoded) do
        if fixture.types[i] == "float" then
          local delta = 0.0000001
          assert.True(math.abs(v - fixture.values[i]) < delta)
        else
          assert.equal(fixture.values[i], v)
        end
      end
    end
  end)

  it("should encode and decode a [list]", function()
    local fixtures = {
      { value_type = "text", value = {"abc", "def"} },
      { value_type = "int", value = {0, 1, 2, 42, -42} },
    }

    for _, fixture in ipairs(fixtures) do
      local encoded = marshall_v3.value_representation(fixture.value, marshall_v3.TYPES.list)
      local buffer = unsmarshall_v3.create_buffer(encoded)

      local value_type = { id = marshall_v3.TYPES[fixture.value_type] }

      local decoded = unsmarshall_v3.read_value(buffer, {
        id = marshall_v3.TYPES.list,
        value = value_type
      })
      assert.same(fixture.value, decoded)
    end
  end)

  it("should encode and decode a [map]", function()
    local fixtures = {
      { key_type = "text", value_type = "text", value = {k1='v1', k2='v2'} },
      { key_type = "text", value_type = "int", value = {k1=1, k2=2} },
      { key_type = "text", value_type = "int", value = {} },
    }

    for _, fixture in ipairs(fixtures) do
      local encoded = marshall_v3.value_representation(fixture.value, marshall_v3.TYPES.map)
      local buffer = unsmarshall_v3.create_buffer(encoded)

      local key_type = { id = marshall_v3.TYPES[fixture.key_type] }
      local value_type = { id = marshall_v3.TYPES[fixture.value_type] }

      local decoded = unsmarshall_v3.read_value(buffer, {
        id = marshall_v3.TYPES.map,
        value = {key_type, value_type}
      })
      assert.same(fixture.value, decoded)
    end
  end)

end)
