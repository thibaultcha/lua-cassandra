package.path = "src/?.lua;"..package.path
local inspect = require "inspect"
local cassandra = require "cassandra"
local log = require "cassandra.log"

log.set_lvl("ERR")

local ok, err = cassandra.spawn_cluster {
  shm = "cassandra",
  contact_points = {"127.0.0.1", "127.0.0.2"}
}
assert(err == nil, inspect(err))

local session, err = cassandra.spawn_session {
  shm = "cassandra"
}
assert(err == nil, inspect(err))

local i = 0
while true do
  i = i + 1
  local res, err = session:execute("SELECT peer FROM system.peers")
  if err then
    print(inspect(err))
    error()
  end
  print("Request "..i.." successful.")
end

