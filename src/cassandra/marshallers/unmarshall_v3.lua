local Marshall_v3 = require "cassandra.marshallers.marshall_v3"
local Unmarshall_v2 = require "cassandra.marshallers.unmarshall_v2"

local _M = Unmarshall_v2:extend()

function _M:read_list(bytes, type)
  local element_type = type.value
  local buffer = self:create_buffer(bytes)
  local n = self:read_int(buffer)
  local elements = {}
  for _ = 1, n do
    elements[#elements + 1] = self:read_value(buffer, element_type)
  end
  return elements
end

function _M:read_map(bytes, type)
  local key_type = type.value[1]
  local value_type = type.value[2]
  local buffer = self:create_buffer(bytes)
  local n = self:read_int(buffer)
  local map = {}
  for _ = 1, n do
    local key = self:read_value(buffer, key_type)
    local value = self:read_value(buffer, value_type)
    map[key] = value
  end
  return map
end

function _M:read_udt(bytes, type)
  local udt = {}
  local buffer = self:create_buffer(bytes)
  for _, field in ipairs(type.fields) do
    local value = self:read_value(buffer, field.type)
    udt[field.name] = value
  end
  return udt
end

function _M:read_tuple(bytes, type)
  local tuple = {}
  local buffer = self:create_buffer(bytes)
  for _, field in ipairs(type.fields) do
    tuple[#tuple + 1] = self:read_value(buffer, field.type)
  end
  return tuple
end

_M.decoders = {
  -- custom=0x00,
  [Marshall_v3.TYPES.ascii]=_M.read_raw,
  [Marshall_v3.TYPES.bigint]=_M.read_bigint,
  [Marshall_v3.TYPES.blob]=_M.read_raw,
  [Marshall_v3.TYPES.boolean]=_M.read_boolean,
  [Marshall_v3.TYPES.counter]=_M.read_bigint,
  -- decimal=0x06,
  [Marshall_v3.TYPES.double]=_M.read_double,
  [Marshall_v3.TYPES.float]=_M.read_float,
  [Marshall_v3.TYPES.int]=_M.read_signed_number,
  [Marshall_v3.TYPES.text]=_M.read_raw,
  [Marshall_v3.TYPES.timestamp]=_M.read_bigint,
  [Marshall_v3.TYPES.uuid]=_M.read_uuid,
  [Marshall_v3.TYPES.varchar]=_M.read_raw,
  [Marshall_v3.TYPES.varint]=_M.read_signed_number,
  [Marshall_v3.TYPES.timeuuid]=_M.read_uuid,
  [Marshall_v3.TYPES.inet]=_M.read_inet,
  [Marshall_v3.TYPES.list]=_M.read_list,
  [Marshall_v3.TYPES.map]=_M.read_map,
  [Marshall_v3.TYPES.set]=_M.read_list,
  [Marshall_v3.TYPES.udt]=_M.read_udt,
  [Marshall_v3.TYPES.tuple]=_M.read_tuple
}

function _M:read_value(buffer, type)
  local bytes = self:read_bytes(buffer)
  if bytes == nil then
    return nil
  end

  return _M.decoders[type.id](self, bytes, type)
end

local function read_udt_type(self, buffer, type, column_name)
  local udt_ksname = self:read_string(buffer)
  local udt_name = self:read_string(buffer)
  local n = self:read_short(buffer)
  local fields = {}
  for _ = 1, n do
    fields[#fields + 1] = {
      name = self:read_string(buffer),
      type = self:read_option(buffer)
    }
  end
  return {
    id = type.id,
    udt_name = udt_name,
    udt_keyspace = udt_ksname,
    name = column_name,
    fields = fields
  }
end

local function read_tuple_type(self, buffer, type, column_name)
  local n = self:read_short(buffer)
  local fields = {}
  for _ = 1, n do
    fields[#fields + 1] = {
      type = self:read_option(buffer)
    }
  end
  return {
    id = type.id,
    name = column_name,
    fields = fields
  }
end

_M.type_decoders = {
  [Marshall_v3.TYPES.udt] = read_udt_type,
  [Marshall_v3.TYPES.tuple] = read_tuple_type
}

return _M
