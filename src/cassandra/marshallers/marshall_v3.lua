local utils = require "cassandra.utils"
local constants = require "cassandra.constants.constants_v3"
local Marshall_v2 = require "cassandra.marshallers.marshall_v2"

local _M = Marshall_v2:extend()

_M.TYPES.udt = 0x30
_M.TYPES.tuple = 0x31

function _M:list_representation(elements)
  local buffer = {self:int_representation(#elements)}
  for _, value in ipairs(elements) do
    buffer[#buffer + 1] = self:value_representation(value)
  end
  return table.concat(buffer)
end

function _M:set_representation(elements)
  return self:list_representation(elements)
end

function _M:map_representation(map)
  local buffer = {}
  local size = 0
  for key, value in pairs(map) do
    buffer[#buffer + 1] = self:value_representation(key)
    buffer[#buffer + 1] = self:value_representation(value)
    size = size + 1
  end
  return self:int_representation(size)..table.concat(buffer)
end

function _M:udt_representation(ordered_fields)
  local buffer = {}
  for _, value in ipairs(ordered_fields) do
    buffer[#buffer + 1] = self:value_representation(value)
  end
  return table.concat(buffer)
end

function _M:tuple_representation(ordered_fields)
  return self:udt_representation(ordered_fields)
end

-- Re-definition
_M.encoders = {
  -- custom=0x00,
  [_M.TYPES.ascii]     = _M.identity_representation,
  [_M.TYPES.bigint]    = _M.bigint_representation,
  [_M.TYPES.blob]      = _M.identity_representation,
  [_M.TYPES.boolean]   = _M.boolean_representation,
  [_M.TYPES.counter]   = _M.bigint_representation,
  -- decimal=0x06,
  [_M.TYPES.double]    = _M.double_representation,
  [_M.TYPES.float]     = _M.float_representation,
  [_M.TYPES.int]       = _M.int_representation,
  [_M.TYPES.text]      = _M.identity_representation,
  [_M.TYPES.timestamp] = _M.bigint_representation,
  [_M.TYPES.uuid]      = _M.uuid_representation,
  [_M.TYPES.varchar]   = _M.identity_representation,
  [_M.TYPES.varint]    = _M.int_representation,
  [_M.TYPES.timeuuid]  = _M.uuid_representation,
  [_M.TYPES.inet]      = _M.inet_representation,
  [_M.TYPES.list]      = _M.list_representation,
  [_M.TYPES.map]       = _M.map_representation,
  [_M.TYPES.set]       = _M.set_representation,
  [_M.TYPES.udt]       = _M.udt_representation,
  [_M.TYPES.tuple]     = _M.tuple_representation
}

function _M:value_representation(value, cass_type)
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
      infered_type = self:int_representation(-1)
  elseif value_lua_type == "table" and value.type and value.value then
    -- Value passed as a binded parameter.
    infered_type = _M.TYPES[value.type]
    value = value.value
  else
    infered_type = _M.TYPES.varchar
  end

  local representation = _M.encoders[infered_type](self, value)
  return self:bytes_representation(representation)
end

-- <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
function _M:query_representation(args, options)
  local repr = self.super.query_representation(self, args, options)

  -- TODO timestamp
  -- TODO named values

  return repr
end

-- <type><n><query_1>...<query_n><consistency><flags>[serial_consistency>][<timestamp>]
function _M:batch_representation(batch, options)
  local repr = self.super.batch_representation(self, batch, options)
  local flags_repr = 0

  local serial_consistency = ""
  if options.serial_consistency ~= nil then
    flags_repr = utils.setbit(flags_repr, constants.query_flags.SERIAL_CONSISTENCY)
    serial_consistency = self:short_representation(options.serial_consistency)
  end

  -- TODO timestamp
  -- TODO named values

  return repr..string.char(flags_repr)..serial_consistency
end

return _M
