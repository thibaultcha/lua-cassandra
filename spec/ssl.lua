package.path = "src/?.lua;src/?/init.lua;"..package.path
local inspect = require "inspect"
local cassandra = require "cassandra"
local log = require "cassandra.log"

log.set_lvl("INFO")

local ssl_options = {
  ca = "/Users/thibaultcha/.ccm/sslverify/client.cer.pem",
  certificate = "/Users/thibaultcha/.ccm/sslverify/client.pem",
  key = "/Users/thibaultcha/.ccm/sslverify/client.key",
  verify = true
}

local _, err = cassandra.spawn_cluster {shm = "cassandra", contact_points = {"127.0.0.1:9042"}, ssl_options = ssl_options}
assert(err == nil, inspect(err))

local session, err = cassandra.spawn_session {shm = "cassandra", ssl_options = ssl_options}
assert(err == nil, inspect(err))

local res, err = session:execute("SELECT peer FROM system.peers")
if err then
  error(err)
end

local inspect = require "inspect"
print(inspect(res))
