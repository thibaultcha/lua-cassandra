local cql_types = {
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
  set       = 0x22,
  udt       = 0x30,
  tuple     = 0x31
}

local consistencies = {
  any = 0X0000,
  one = 0X0001,
  two = 0X0002,
  three = 0X0003,
  quorum = 0X0004,
  all = 0X0005,
  local_quorum = 0X0006,
  each_quorum = 0X0007,
  serial = 0X0008,
  local_serial = 0X0009,
  local_one = 0X000a
}

local types_mt = {}

function types_mt:__index(key)
  if cql_types[key] ~= nil then
    return function(value)
      return {value = value, type_id = cql_types[key]}
    end
  end

  return rawget(self, key)
end

return setmetatable({
  cql_types = cql_types,
  consistencies = consistencies
}, types_mt)
