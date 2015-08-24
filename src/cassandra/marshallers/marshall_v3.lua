local utils = require "cassandra.utils"
local constants = require "cassandra.constants.constants_v3"
local Marshall_v2 = require "cassandra.marshallers.marshall_v2"
local big_endian_representation = utils.big_endian_representation

local _M = Marshall_v2:extend()

_M.TYPES.udt = 0x30
_M.TYPES.tuple = 0x31

function _M:long_representation(num)
  return big_endian_representation(num, 8)
end

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

function _M:values_representation(args, named)
  if not args then
    return ""
  elseif named then
    local values = {}
    for name, value in pairs(args) do
      values[#values + 1] = self:string_representation(name)..self:value_representation(value)
    end
    local len = self:short_representation(#values)
    table.insert(values, 1, len)
    return table.concat(values)
  else
    return self.super.values_representation(self, args)
  end
end

-- <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
function _M:query_representation(args, options)
  local consistency_repr = self:short_representation(options.consistency_level)
  local args_representation = ""

  -- <flags>
  local flags = 0
  if args then
    flags = utils.setbit(flags, constants.query_flags.VALUES)
    if utils.is_array(args) ~= -1 then
      args_representation = self:values_representation(args)
    else
      args_representation = self:values_representation(args, true)
      flags = utils.setbit(flags, constants.query_flags.NAMED_VALUES)
    end
  end

  local paging_state = ""
  if options.paging_state then
    flags = utils.setbit(flags, constants.query_flags.PAGING_STATE)
    paging_state = self:bytes_representation(options.paging_state)
  end

  local page_size = ""
  if options.page_size > 0 then
    flags = utils.setbit(flags, constants.query_flags.PAGE_SIZE)
    page_size = self:int_representation(options.page_size)
  end

  local serial_consistency = ""
  if options.serial_consistency ~= nil then
    flags = utils.setbit(flags, constants.query_flags.SERIAL_CONSISTENCY)
    serial_consistency = self:short_representation(options.serial_consistency)
  end

  local timestamp = ""
  if options.timestamp then
    flags = utils.setbit(flags, constants.query_flags.DEFAULT_TIMESTAMP)
    timestamp = self:long_representation(options.timestamp)
  end

  return consistency_repr..string.char(flags)..args_representation..page_size..paging_state..serial_consistency..timestamp
end

-- <type><n><query_1>...<query_n><consistency><flags>[serial_consistency>][<timestamp>]
function _M:batch_representation(batch, options)
  local named_values = false

  local b = {}
  -- <type>
  b[#b + 1] = string.char(batch.type)
  -- <n> (number of queries)
  b[#b + 1] = self:short_representation(#batch.queries)
  -- <query_i> (operations)
  for _, query in ipairs(batch.queries) do
    local kind, string_or_id
    if type(query.query) == "string" then
      kind = self:boolean_representation(false)
      string_or_id = self:long_string_representation(utils.trim(query.query))
    else
      kind = self:boolean_representation(true)
      string_or_id = self:short_bytes_representation(query.query.id)
    end

    -- <kind><string_or_id><n>[<name_1>]<value_1>...[<name_n>]<value_n> (n can be 0, but is required)
    if query.args then
      if utils.is_array(query.args) ~= -1 then
        b[#b + 1] = kind..string_or_id..self:values_representation(query.args)
      else
        -- if true for one query_i, should be true for all
        named_values = true
        b[#b + 1] = kind..string_or_id..self:values_representation(query.args, true)
      end
    else
      b[#b + 1] = kind..string_or_id..self:short_representation(0)
    end
  end

  -- <consistency>
  b[#b + 1] = self:short_representation(options.consistency_level)

  -- <flags>
  local flags = 0

  -- [<serial_consistency>]
  local serial_consistency = ""
  if options.serial_consistency ~= nil then
    flags = utils.setbit(flags, constants.query_flags.SERIAL_CONSISTENCY)
    serial_consistency = self:short_representation(options.serial_consistency)
  end

  -- [<timestamp>]
  local timestamp = ""
  if options.timestamp ~= nil then
    flags = utils.setbit(flags, constants.query_flags.DEFAULT_TIMESTAMP)
    timestamp = self:long_representation(options.timestamp)
  end

  if named_values then
    flags = utils.setbit(flags, constants.query_flags.NAMED_VALUES)
  end

  b[#b + 1] = string.char(flags)
  b[#b + 1] = serial_consistency
  b[#b + 1] = timestamp

  return table.concat(b)
end

return _M
