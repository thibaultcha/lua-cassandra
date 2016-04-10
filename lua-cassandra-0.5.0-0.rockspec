package = "lua-cassandra"
version = "0.5.0-0"
source = {
  url = "git://github.com/thibaultCha/lua-cassandra",
  tag = "0.5.0"
}
description = {
  summary = "Feature-rich client library for Cassandra",
  homepage = "http://thibaultcha.github.io/lua-cassandra",
  license = "MIT"
}
build = {
  type = "builtin",
  modules = {
    ["cassandra"] = "cassandra/init.lua",
    ["cassandra.cql"] = "cassandra/cql.lua",
    ["cassandra.auth"] = "cassandra/auth.lua",
    ["cassandra.socket"] = "cassandra/socket.lua",
    ["cassandra.cluster"] = "cassandra/cluster.lua",

    ["cassandra.policies.retry"] = "src/cassandra/policies/retry.lua",
    ["cassandra.policies.reconnection"] = "src/cassandra/policies/reconnection.lua",
    ["cassandra.policies.load_balancing"] = "src/cassandra/policies/load_balancing.lua",

    ["cassandra.utils.shm"] = "src/cassandra/utils/shm.lua",
    ["cassandra.utils.bit"] = "src/cassandra/utils/bit.lua",
    ["cassandra.utils.time"] = "src/cassandra/utils/time.lua",
    ["cassandra.utils.table"] = "src/cassandra/utils/table.lua",
  }
}
