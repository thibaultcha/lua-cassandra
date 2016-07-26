--------------------------------
-- Single host module, plain Lua
--------------------------------

local cassandra = require "cassandra"

local client = assert(cassandra.new {
  host = "127.0.0.1",
  keyspace = "my_keyspace"
})

client:settimeout(1000)

assert(client:connect())

assert(client:execute [[
  CREATE TABLE IF NOT EXISTS things(
    id uuid PRIMARY KEY,
    n int
  )
]])

assert(client:execute [[
  CREATE TABLE IF NOT EXISTS counters(
    key text PRIMARY KEY,
    value counter
  )
]])

-- Logged batch
local res = assert(client:batch {
  {"INSERT INTO things(id, n) VALUES(1144bada-852c-11e3-89fb-e0b9a54a6d11, 1)"},
  {"UPDATE things SET n = 2 WHERE id = 1144bada-852c-11e3-89fb-e0b9a54a6d11"},
  {"UPDATE things SET n = 3 WHERE id = 1144bada-852c-11e3-89fb-e0b9a54a6d11"}
})
print(res.type) -- "VOID"

-- Unlogged batch, with binded parameters
local uuid = "1144bada-852c-11e3-89fb-e0b9a54a6d11"
local serialized_uuid = cassandra.uuid(uuid)

res = assert(client:batch({
  {"INSERT INTO things(id, n) VALUES(?, ?)", {serialized_uuid, 4}},
  {"INSERT INTO things(id, n) VALUES(?, ?)", {serialized_uuid, 5}},
  {"INSERT INTO things(id, n) VALUES(?, ?)", {serialized_uuid, 6}},
}), {
  logged = false
})
print(res.type) -- "VOID"

-- Counter batch
res = assert(client:batch({
  {"UPDATE counters SET value = value + 1 WHERE key = 'counter'"},
  {"UPDATE counters SET value = value + 1 WHERE key = 'counter'"},
  {"UPDATE counters SET value = value + 1 WHERE key = 'counter'"}
}, {
  counter = true
}))
print(res.type) -- "VOID"

-- Batch of prepared queries
local res1 = assert(client:prepare("INSERT INTO things(id, n) VALUES(1144bada-852c-11e3-89fb-e0b9a54a6d11, 1)"))
local res2 = assert(client:prepare("UPDATE things set n = 2 WHERE id = 1144bada-852c-11e3-89fb-e0b9a54a6d11"))

local res = assert(client:batch({
  {[3] = res1.query_id},
  {[3] = res2.query_id}
}, {
  prepared = true
}))
print(res.type) -- "VOID"

client:close()
