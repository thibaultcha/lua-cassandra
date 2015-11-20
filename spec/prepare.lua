package.path = "src/?.lua;src/?/init.lua;"..package.path
local inspect = require "inspect"
local cassandra = require "cassandra"
local log = require "cassandra.log"

log.set_lvl("INFO")

local _, err = cassandra.spawn_cluster {shm = "cassandra", contact_points = {"127.0.0.1", "127.0.0.2"}}
assert(err == nil, inspect(err))

local session, err = cassandra.spawn_session {shm = "cassandra", keyspace = "page"}
assert(err == nil, inspect(err))

--
--
--

local rows, err = session:execute("SELECT * FROM users", nil, {prepare = true})
if err then
  error(err)
end

print(#rows)

local rows, err = session:execute("SELECT * FROM users", nil, {prepare = true})
if err then
  error(err)
end

print(#rows)
