package.path = "src/?.lua;src/?/init.lua;"..package.path
local inspect = require "inspect"
local cassandra = require "cassandra"
local log = require "cassandra.log"

log.set_lvl("INFO")

local _, err = cassandra.spawn_cluster {shm = "cassandra", contact_points = {"127.0.0.1", "127.0.0.2"}}
assert(err == nil, inspect(err))

local session, err = cassandra.spawn_session {shm = "cassandra"}
assert(err == nil, inspect(err))

local _, err = session:execute([[
  CREATE KEYSPACE IF NOT EXISTS page
  WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2}
]])
assert(err == nil, inspect(err))

os.execute("sleep 1")

local _, err = session:execute [[
  CREATE TABLE IF NOT EXISTS page.users(
    id uuid PRIMARY KEY,
    name varchar,
    age int
  )
]]
assert(err == nil, inspect(err))

local _, err = session:set_keyspace("page")
assert(err == nil, inspect(err))

os.execute("sleep 1")

for i = 1, 10000 do
  --local _, err = session:execute("INSERT INTO users(id, name, age) VALUES(uuid(), ?, ?)", {"Alice", 33})
  --assert(err == nil, inspect(err))
end

local rows, err = session:execute("SELECT COUNT(*) FROM users")
assert(err == nil, inspect(err))
print(rows[1].count)

local rows, err = session:execute("SELECT * FROM users")
assert(err == nil, inspect(err))
print(inspect(rows))
