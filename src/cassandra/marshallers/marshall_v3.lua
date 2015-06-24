local marshall_v2 = require "cassandra.marshallers.marshall_v2"

local _M = {}

for k, v in pairs(marshall_v2) do
  _M[k] = v
end

-- Extend

_M.TYPES.udt = 0x30
_M.TYPES.tuple = 0x31

function _M.list_representation(elements)
  local buffer = {_M.int_representation(#elements)}
  for _, value in ipairs(elements) do
    buffer[#buffer + 1] = _M.value_representation(value)
  end
  return table.concat(buffer)
end

function _M.map_representation(map)
  local buffer = {}
  local size = 0
  for key, value in pairs(map) do
    buffer[#buffer + 1] = _M.value_representation(key)
    buffer[#buffer + 1] = _M.value_representation(value)
    size = size + 1
  end
  return _M.int_representation(size) .. table.concat(buffer)
end

function _M.udt_representation(ordered_fields)
  local buffer = {}
  for _, value in ipairs(ordered_fields) do
    buffer[#buffer + 1] = _M.value_representation(value)
  end
  return table.concat(buffer)
end

function _M.tuple_representation(ordered_fields)
  return _M.udt_representation(ordered_fields)
end

_M.encoders[_M.TYPES.udt]   = _M.udt_representation
_M.encoders[_M.TYPES.tuple] = _M.tuple_representation

function _M.value_representation(value, cass_type)
  local infered_type
  local value_lua_type = type(value)
  if cass_type then
    infered_type = cass_type
  elseif value_lua_type == "number" and math.floor(value) == value then
    infered_type = _M.TYPES.int
  elseif value_lua_type == "number" then
    infered_type = _M.TYPES.float
  elseif value_lua_type == "boolean" then
    infered_type = _M.TYPES.boolean
  elseif value_lua_type == "table" and value.type == "null" then
      infered_type = _M.int_representation(-1)
  elseif value_lua_type == "table" and value.type and value.value then
    -- Value passed as a binded parameter.
    infered_type = _M.TYPES[value.type]
    value = value.value
  else
    infered_type = _M.TYPES.varchar
  end

  local representation = _M.encoders[infered_type](value)
  return _M.bytes_representation(representation)
end

return _M
