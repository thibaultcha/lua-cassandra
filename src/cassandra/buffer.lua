local Buffer = require "cassandra.utils.buffer"
local CQL_TYPES = require "cassandra.types.cql_types"
local t_utils = require "cassandra.utils.table"
local math_floor = math.floor

--- Frame types
-- @section frame_types

local TYPES = {
  "byte",
  "int",
  -- "long",
  "short",
  "string",
  "long_string",
  "uuid",
  -- "string_list",
  "bytes",
  -- "short_bytes",
  "options",
  -- "options_list"
  "inet",
  -- "consistency"
  "string_map",
  -- "string_multimap"
}

for _, buf_type in ipairs(TYPES) do
  local mod = require("cassandra.types."..buf_type)
  Buffer["read_"..buf_type] = mod.read
  Buffer["repr_"..buf_type] = mod.repr
  Buffer["write_"..buf_type] = function(self, val)
    local repr = mod.repr(self, val)
    self:write(repr)
  end
end

--- CQL Types
-- @section cql_types

local CQL_DECODERS = {
  -- custom = 0x00,
  [CQL_TYPES.ascii] = "raw",
  -- [CQL_TYPES.bigint] = "bigint",
  [CQL_TYPES.blob] = "raw",
  [CQL_TYPES.boolean] = "boolean",
  -- [CQL_TYPES.counter] = "counter",
  -- decimal 0x06
  -- [CQL_TYPES.double] = "double",
  -- [CQL_TYPES.float] = "float",
  [CQL_TYPES.inet] = "inet",
  [CQL_TYPES.int] = "int",
  [CQL_TYPES.text] = "raw",
  [CQL_TYPES.list] = "set",
  [CQL_TYPES.map] = "map",
  [CQL_TYPES.set] = "set",
  [CQL_TYPES.uuid] = "uuid",
  -- [CQL_TYPES.timestamp] = "timestamp",
  [CQL_TYPES.varchar] = "raw",
  -- [CQL_TYPES.varint] = "varint",
  -- [CQL_TYPES.timeuuid] = "timeuuid",
  -- [CQL_TYPES.udt] = "udt",
  -- [CQL_TYPES.tuple] = "tuple"
}

for _, cql_decoder in pairs(CQL_DECODERS) do
  local mod = require("cassandra.types."..cql_decoder)
  Buffer["repr_cql_"..cql_decoder] = function(self, ...)
    local repr = mod.repr(self, ...)
    return self:repr_bytes(repr)
  end
  Buffer["write_cql_"..cql_decoder] = function(self, ...)
    local repr = mod.repr(self, ...)
    self:write_bytes(repr)
  end
  Buffer["read_cql_"..cql_decoder] = function(self, ...)
    local bytes = self:read_bytes()
    local buf = Buffer(self.version, bytes)
    return mod.read(buf, ...)
  end
end

function Buffer:repr_cql_value(value, assumed_type)
  local infered_type
  local lua_type = type(value)

  if assumed_type then
    infered_type = assumed_type
  elseif lua_type == "number" and math_floor(value) == value then
    infered_type = CQL_TYPES.int
  elseif lua_type == "table" then
    if t_utils.is_array(value) then
      infered_type = CQL_TYPES.set
    else
      infered_type = CQL_TYPES.map
    end
  else
    infered_type = CQL_TYPES.varchar
  end

  local encoder = "repr_cql_"..CQL_DECODERS[infered_type]
  return Buffer[encoder](self, value)
end

function Buffer:write_cql_value(...)
  self:write(self:repr_cql_value(...))
end

function Buffer:read_cql_value(assumed_type)
  local decoder = "read_cql_"..CQL_DECODERS[assumed_type.id]
  return Buffer[decoder](self, assumed_type.value)
end

return Buffer
