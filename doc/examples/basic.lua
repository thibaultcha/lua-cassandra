--------
-- Basic example using LuaSocket in plain Lua

local cassandra = require "cassandra"

-- If this `shm` is empty, this will connect to the cluster
-- and retrieve its infos first.
local session, err = cassandra.spawn_session {
  shm = "cassandra", -- used to store cluster infos
  contact_points = {"127.0.0.1", "127.0.0.2", "127.0.0.3"}, -- entry points to your cluster
  keyspace = "my_keyspace", -- this keyspace must exist
  socket_options = {
    connect_timeout = 5000, -- 5s timeout for connect
    read_timeout = 8000 -- 8s timeout for operations
  }
}
assert(err == nil)

-- simple query
local result, err = session:execute [[
  CREATE TABLE users(
    id uuid PRIMARY KEY,
    name varchar,
    age int
  )
]]
assert(err == nil)
assert(result.type == "SCHEMA_CHANGE")

-- query with arguments
local ok, err = session:execute("INSERT INTO users(id, name, age) VALUES(?, ?, ?)", {
  cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11"),
  "John O'Reilly",
  42
})

-- select statement with some custom query options, overriding
-- the ones defined at session creation
local rows, err = session:execute("SELECT id, name, age FROM users", nil, {
  consistency = cassandra.concistencies.local_one, -- desired consistency (default is 'ONE')
  page_size = 100 -- no more than 100 rows
}
assert(1 == #rows)

local user = rows[1]
print(user.name) -- "John O'Reilly"
print(user.age) -- 42
print(user.id) -- "1144bada-852c-11e3-89fb-e0b9a54a6d11"

session:shutdown()

--------
-- Basic example in ngx_lua

http {

  # will store cluster infos
  lua_shared_dict cassandra 1m;

  server {
    ...

    location / {

      content_by_lua_block {
        local cassandra = require "cassandra"

        local session, err = cassandra.spawn_session {
          shm = "cassandra", -- name of the shared dict
          contact_points = {"127.0.0.1", "127.0.0.2", "127.0.0.3"},
          keyspace = "my_keyspace", -- this keyspace must exist
          socket_options = {
            connect_timeout = 5000, -- 5s timeout for connect
            read_timeout = 8000 -- 8s timeout for operations
          }
        }
        if err then
          ngx.log(ngx.ERR, err)
          ngx.exit(500)
        end

        local rows, err = session:execute("SELECT * FROM users")
        if err then
          -- ...
        end

        for _, row in ipairs(rows) do
          ngx.say(row.name..": "..row.age)
        end

        -- keep the sockets alive in the connection pool
        session:set_keep_alive()
      }
    }
  }
}
