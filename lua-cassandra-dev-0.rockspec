package = "lua-cassandra"
version = "dev-0"
source = {
  url = "git://github.com/thibaultCha/lua-cassandra"
}
description = {
  summary = "A pure Lua client library for Apache Cassandra",
  homepage = "https://github.com/thibaultCha/lua-cassandra",
  license = "MIT"
}
dependencies = {
  "luabitop"
}
build = {
  type = "builtin",
  modules = {
    ["cassandra"] = "lib/cassandra/init.lua",
    ["cassandra.cql"] = "lib/cassandra/cql.lua",
    ["cassandra.auth"] = "lib/cassandra/auth.lua",
    ["cassandra.socket"] = "lib/cassandra/socket.lua",

    ["resty.cassandra.cluster"] = "lib/resty/cassandra/cluster.lua",
    ["resty.cassandra.policies.lb"] = "lib/resty/cassandra/policies/lb/init.lua",
    ["resty.cassandra.policies.lb.rr"] = "lib/resty/cassandra/policies/lb/rr.lua",
    ["resty.cassandra.policies.lb.dc_rr"] = "lib/resty/cassandra/policies/lb/dc_rr.lua",
    ["resty.cassandra.policies.reconnection"] = "lib/resty/cassandra/policies/reconnection/init.lua",
    ["resty.cassandra.policies.reconnection.exp"] = "lib/resty/cassandra/policies/reconnection/exp.lua",
    ["resty.cassandra.policies.reconnection.const"] = "lib/resty/cassandra/policies/reconnection/const.lua",
    ["resty.cassandra.policies.retry"] = "lib/resty/cassandra/policies/retry/init.lua",
    ["resty.cassandra.policies.retry.simple"] = "lib/resty/cassandra/policies/retry/simple.lua"
  }
}
