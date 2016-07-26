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

local res = assert(client:execute [[
  CREATE TABLE users(
    id uuid PRIMARY KEY,
    name varchar,
    age int
  )
]])
print(res.type) -- "SCHEMA_CHANGE"

res = assert(client:execute("INSERT INTO users(id, name, age) VALUES(?, ?, ?)", {
  cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11"),
  "John O Reilly",
  42
}))
print(res.type) -- "VOID"

local rows = assert(client:execute("SELECT * FROM users WHERE age = ?", {
  age = 42 -- key/value args
}, {
  named = true -- named arguments
}))
print(rows.type)    -- "ROWS"
print(#rows)        -- 1
print(rows[1].name) -- "John O Reilly"

client:close()

----------------------------
-- Cluster module, OpenResty
----------------------------

http {
  # shm storing cluster information
  lua_shared_dict cassandra 1m;

  server {
    ...

    location / {
      content_by_lua_block {
        local cassandra = require "cassandra"
        local Cluster = require "resty.cassandra.cluster"

        local cluster, err = Cluster.new {
          shm = "cassandra", -- defined in http block
          contact_points = {"127.0.0.1", "127.0.0.2"},
          keyspace = "my_keyspace"
        }
        if not cluster then
          ngx.log(ngx.ERR, "could not create cluster: ", err)
          ngx.exit(500)
        end

        local rows, err = cluster.execute("SELECT * FROM users WHERE id = ? AND name = ?", {
          cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11"),
          "John O Reilly"
        })
        if not rows then
          ngx.log(ngx.ERR, "could not retrieve users: ", err)
          ngx.exit(500)
        end

        for i, row in ipairs(rows) do
          ngx.say(i, ": ", rows[i].name) -- "1: John O Reilly"
        end
      }
    }
  }
}
