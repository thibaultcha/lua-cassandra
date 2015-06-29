--------
-- Basic example

local cassandra = require "cassandra"
-- local cassandra = require "cassandra.v2" -- binary protocol v2 for Cassandra 2.0.x

local session = cassandra:new()
session:set_timeout(1000) -- 1000ms timeout

local connected, err = session:connect("127.0.0.1", 9042)
assert(connected)
session:set_keyspace("demo")

-- simple query
local table_created, err = session:execute [[
  CREATE TABLE users(
    id uuid PRIMARY KEY,
    name varchar,
    age int
  )
]]

-- query with arguments
local ok, err = session:execute("INSERT INTO users(name, age, user_id) VALUES(?, ?, ?)"
  , {"John O'Reilly", 42, cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11")})


-- select statement
local users, err = session:execute("SELECT name, age, user_id FROM users")
assert(1 == #users)

local user = users[1]
print(user.name) -- "John O'Reilly"
print(user.user_id) -- "1144bada-852c-11e3-89fb-e0b9a54a6d11"
print(user.age) -- 42
