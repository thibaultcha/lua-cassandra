package.path = "src/?.lua;src/?/init.lua;"..package.path
local inspect = require "inspect"
local cassandra = require "cassandra"
local log = require "cassandra.log"

log.set_lvl("ERR")

local _, err = cassandra.spawn_cluster {shm = "cassandra", contact_points = {"127.0.0.1", "127.0.0.2"}}
assert(err == nil, inspect(err))

local session, err = cassandra.spawn_session {shm = "cassandra", keyspace = "page"}
assert(err == nil, inspect(err))

local _, err = session:execute [[
    CREATE KEYSPACE IF NOT EXISTS stuff
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
]]
if err then
  error(err)
end
