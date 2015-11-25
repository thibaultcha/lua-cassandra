package = "lua-cassandra"
version = "0.4.0-0"
source = {
  url = "git://github.com/thibaultCha/lua-cassandra",
  tag = "0.4.0"
}
description = {
  summary = "Lua Cassandra client library",
  homepage = "http://thibaultcha.github.io/lua-cassandra",
  license = "MIT"
}
dependencies = {
  "luasocket ~> 2.0.2-6",
  "lua-cjson ~> 2.1.0-1"
}
build = {
  type = "builtin",
  modules = {
    ["cassandra"] = "src/cassandra.lua",

    ["cassandra.log"] = "src/cassandra/log.lua",
    ["cassandra.cache"] = "src/cassandra/cache.lua",
    ["cassandra.errors"] = "src/cassandra/errors.lua",
    ["cassandra.options"] = "src/cassandra/options.lua",
    ["cassandra.requests"] = "src/cassandra/requests.lua",
    ["cassandra.frame_reader"] = "src/cassandra/frame_reader.lua",

    ["cassandra.buffer"] = "src/cassandra/buffer/init.lua",
    ["cassandra.buffer.raw_buffer"] = "src/cassandra/buffer/raw_buffer.lua",

    ["cassandra.policies.retry"] = "src/cassandra/policies/retry.lua",
    ["cassandra.policies.reconnection"] = "src/cassandra/policies/reconnection.lua",
    ["cassandra.policies.load_balancing"] = "src/cassandra/policies/load_balancing.lua",
    ["cassandra.policies.address_resolution"] = "src/cassandra/policies/address_resolution.lua",

    ["cassandra.auth"] = "src/cassandra/auth/init.lua",
    ["cassandra.auth.plain_text_password"] = "src/cassandra/auth/plain_text_password.lua",

    ["cassandra.utils.bit"] = "src/cassandra/utils/bit.lua",
    ["cassandra.utils.time"] = "src/cassandra/utils/time.lua",
    ["cassandra.utils.table"] = "src/cassandra/utils/table.lua",
    ["cassandra.utils.number"] = "src/cassandra/utils/number.lua",
    ["cassandra.utils.string"] = "src/cassandra/utils/string.lua",
    ["cassandra.utils.classic"] = "src/cassandra/utils/classic.lua",

    ["cassandra.types"] = "src/cassandra/types/init.lua",
    ["cassandra.types.bigint"] = "src/cassandra/types/bigint.lua",
    ["cassandra.types.boolean"] = "src/cassandra/types/boolean.lua",
    ["cassandra.types.byte"] = "src/cassandra/types/byte.lua",
    ["cassandra.types.bytes"] = "src/cassandra/types/bytes.lua",
    ["cassandra.types.double"] = "src/cassandra/types/double.lua",
    ["cassandra.types.float"] = "src/cassandra/types/float.lua",
    ["cassandra.types.frame_header"] = "src/cassandra/types/frame_header.lua",
    ["cassandra.types.inet"] = "src/cassandra/types/inet.lua",
    ["cassandra.types.int"] = "src/cassandra/types/int.lua",
    ["cassandra.types.long"] = "src/cassandra/types/long.lua",
    ["cassandra.types.long_string"] = "src/cassandra/types/long_string.lua",
    ["cassandra.types.map"] = "src/cassandra/types/map.lua",
    ["cassandra.types.options"] = "src/cassandra/types/options.lua",
    ["cassandra.types.raw"] = "src/cassandra/types/raw.lua",
    ["cassandra.types.set"] = "src/cassandra/types/set.lua",
    ["cassandra.types.short"] = "src/cassandra/types/short.lua",
    ["cassandra.types.short_bytes"] = "src/cassandra/types/short_bytes.lua",
    ["cassandra.types.string"] = "src/cassandra/types/string.lua",
    ["cassandra.types.string_map"] = "src/cassandra/types/string_map.lua",
    ["cassandra.types.tuple"] = "src/cassandra/types/tuple.lua",
    ["cassandra.types.tuple_type"] = "src/cassandra/types/tuple_type.lua",
    ["cassandra.types.udt"] = "src/cassandra/types/udt.lua",
    ["cassandra.types.udt_type"] = "src/cassandra/types/udt_type.lua",
    ["cassandra.types.uuid"] = "src/cassandra/types/uuid.lua"
  }
}
