--------
-- Examples of batched queries
-- @see https://cassandra.apache.org/doc/cql3/CQL-2.2.html#batchStmt

local cassandra = require "cassandra"

local session, err = cassandra.spawn_session {
  shm = "cassandra", -- used to store cluster infos
  contact_points = {"127.0.0.1", "127.0.0.2", "127.0.0.3"}, -- entry points to your cluster
  keyspace = "my_keyspace" -- this keyspace must exist
}
assert(err == nil)

-- Basic logged batch
local res, err = session:batch {
  {"INSERT INTO users(id, name) VALUES(123, 'Alice')"},
  {"UPDATE users SET name = 'Alicia' WHERE id = 123"}
}
assert(err.type == "VOID")

-- With binded parameters and prepared queries
local res, err = session:batch({
  {"INSERT INTO users(id, name) VALUES(?, ?)", {cassandra.uuid(some_uuid), "Alice"}},
  {"UPDATE users SET name = ? WHERE id = ?", {cassandra.uuid(some_uuid), "Alicia"}}
}, {prepare = true})
assert(err.type == "VOID")

-- Unlogged batch
local res, err = session:batch({
  {"INSERT INTO users(id, name) VALUES(123, 'Alice')"},
  {"UPDATE users SET name = 'Alicia' WHERE id = 123"}
}, {logged = false})
assert(err.type == "VOID")

-- Counter batch
local res, err = session:batch({
  {"UPDATE table SET value = value + 1 WHERE key = 'counter'"},
  {"UPDATE table SET value = value + 5 WHERE key = 'counter'"},
  {"UPDATE table SET value = value + 2 WHERE key = ?", {"counter"}}
}, {counter = true})
assert(err.type == "VOID")

session:shutdown()
