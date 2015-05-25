local big_endian_representation = require("cassandra.utils").big_endian_representation

local _M = {}

_M.TYPES = {
  custom    = 0x00,
  ascii     = 0x01,
  bigint    = 0x02,
  blob      = 0x03,
  boolean   = 0x04,
  counter   = 0x05,
  decimal   = 0x06,
  double    = 0x07,
  float     = 0x08,
  int       = 0x09,
  text      = 0x0A,
  timestamp = 0x0B,
  uuid      = 0x0C,
  varchar   = 0x0D,
  varint    = 0x0E,
  timeuuid  = 0x0F,
  inet      = 0x10,
  list      = 0x20,
  map       = 0x21,
  set       = 0x22
}

function _M.identity_representation(value)
  return value
end

function _M.int_representation(num)
  return big_endian_representation(num, 4)
end

function _M.short_representation(num)
  return big_endian_representation(num, 2)
end

function _M.string_representation(str)
  return _M.short_representation(#str) .. str
end

function _M.long_string_representation(str)
  return _M.int_representation(#str) .. str
end

function _M.bytes_representation(bytes)
  return _M.int_representation(#bytes) .. bytes
end

function _M.short_bytes_representation(bytes)
  return _M.short_representation(#bytes) .. bytes
end

function _M.boolean_representation(value)
  return value and "\001" or "\000"
end

function _M.bigint_representation(n)
  local first_byte = n >= 0 and 0 or 0xFF
  return string.char(first_byte, -- only 53 bits from double
                     math.floor(n / 0x1000000000000) % 0x100,
                     math.floor(n / 0x10000000000) % 0x100,
                     math.floor(n / 0x100000000) % 0x100,
                     math.floor(n / 0x1000000) % 0x100,
                     math.floor(n / 0x10000) % 0x100,
                     math.floor(n / 0x100) % 0x100,
                     n % 0x100)
end

function _M.uuid_representation(value)
  local str = string.gsub(value, "-", "")
  local buffer = {}
  for i = 1, #str, 2 do
    local byte_str =  string.sub(str, i, i + 1)
    buffer[#buffer + 1] = string.char(tonumber(byte_str, 16))
  end
  return table.concat(buffer)
end

-- 'inspired' by https://github.com/fperrad/lua-MessagePack/blob/master/src/MessagePack.lua
function _M.double_representation(number)
  local sign = 0
  if number < 0.0 then
    sign = 0x80
    number = -number
  end
  local mantissa, exponent = math.frexp(number)
  if mantissa ~= mantissa then
    return string.char(0xFF, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- nan
  elseif mantissa == math.huge then
    if sign == 0 then
      return string.char(0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- +inf
    else
      return string.char(0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- -inf
    end
  elseif mantissa == 0.0 and exponent == 0 then
    return string.char(sign, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- zero
  else
    exponent = exponent + 0x3FE
    mantissa = (mantissa * 2.0 - 1.0) * math.ldexp(0.5, 53)
    return string.char(sign + math.floor(exponent / 0x10),
                       (exponent % 0x10) * 0x10 + math.floor(mantissa / 0x1000000000000),
                       math.floor(mantissa / 0x10000000000) % 0x100,
                       math.floor(mantissa / 0x100000000) % 0x100,
                       math.floor(mantissa / 0x1000000) % 0x100,
                       math.floor(mantissa / 0x10000) % 0x100,
                       math.floor(mantissa / 0x100) % 0x100,
                       mantissa % 0x100)
  end
end

function _M.float_representation(number)
  if number == 0 then
    return string.char(0x00, 0x00, 0x00, 0x00)
  elseif number ~= number then
    return string.char(0xFF, 0xFF, 0xFF, 0xFF)
  else
    local sign = 0x00
    if number < 0 then
      sign = 0x80
      number = -number
    end
    local mantissa, exponent = math.frexp(number)
    exponent = exponent + 0x7F
    if exponent <= 0 then
      mantissa = math.ldexp(mantissa, exponent - 1)
      exponent = 0
    elseif exponent > 0 then
      if exponent >= 0xFF then
        return string.char(sign + 0x7F, 0x80, 0x00, 0x00)
      elseif exponent == 1 then
        exponent = 0
      else
        mantissa = mantissa * 2 - 1
        exponent = exponent - 1
      end
    end
    mantissa = math.floor(math.ldexp(mantissa, 23) + 0.5)
    return string.char(sign + math.floor(exponent / 2),
                       (exponent % 2) * 0x80 + math.floor(mantissa / 0x10000),
                       math.floor(mantissa / 0x100) % 0x100,
                       mantissa % 0x100)
  end
end

function _M.inet_representation(value)
  local digits = {}
  -- ipv6
  for d in string.gmatch(value, "([^:]+)") do
    if #d == 4 then
      for i = 1, #d, 2 do
        digits[#digits + 1] = string.char(tonumber(string.sub(d, i, i + 1), 16))
      end
    end
  end
  -- ipv4
  if #digits == 0 then
    for d in string.gmatch(value, "(%d+)") do
      table.insert(digits, string.char(d))
    end
  end
  return table.concat(digits)
end

function _M.list_representation(elements)
  local buffer = {_M.short_representation(#elements)}
  for _, value in ipairs(elements) do
    buffer[#buffer + 1] = _M.value_representation(value, nil, true)
  end
  return table.concat(buffer)
end

function _M.set_representation(elements)
  return _M.list_representation(elements)
end

function _M.map_representation(map)
  local buffer = {}
  local size = 0
  for key, value in pairs(map) do
    buffer[#buffer + 1] = _M.value_representation(key, nil, true)
    buffer[#buffer + 1] = _M.value_representation(value, nil, true)
    size = size + 1
  end
  return _M.short_representation(size) .. table.concat(buffer)
end

function _M.string_map_representation(map)
  local buffer = {}
  local n = 0
  for k, v in pairs(map) do
    buffer[#buffer + 1] = _M.string_representation(k)
    buffer[#buffer + 1] = _M.string_representation(v)
    n = n + 1
  end
  return _M.short_representation(n) .. table.concat(buffer)
end

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
  [_M.TYPES.set]       = _M.set_representation
}

function _M.value_representation(value, cass_type, short)
  local infered_type
  local value_lua_type = type(value)
  if cass_type then
    infered_type = cass_type
  elseif value_lua_type == 'number' and math.floor(value) == value then
    infered_type = _M.TYPES.int
  elseif value_lua_type == 'number' then
    infered_type = _M.TYPES.float
  elseif value_lua_type == 'boolean' then
    infered_type = _M.TYPES.boolean
  elseif value_lua_type == 'table' and value.type == 'null' then
    if short then
      infered_type = _M.short_representation(-1)
    else
      infered_type = _M.int_representation(-1)
    end
  elseif value_lua_type == 'table' and value.type and value.value then
    value = value.value
  elseif value_lua_type == 'table' and value.type then
    infered_type = _M.TYPES[value.type]
  else
    infered_type = _M.TYPES.varchar
  end

  local representation = _M.encoders[infered_type](value)

  if short then
    return _M.short_bytes_representation(representation)
  else
    return _M.bytes_representation(representation)
  end
end

function _M.values_representation(args)
  if not args then
    return ""
  end
  local values = {_M.short_representation(#args)}
  for _, value in ipairs(args) do
    values[#values + 1] = _M.value_representation(value)
  end
  return table.concat(values)
end

return _M
