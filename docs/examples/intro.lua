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

-------------------------------------------------------------------------------
-- Cluster module, OpenResty
-- This approach allows the cluster to live as an upvalue in your module's main
-- chunk, assuming the `lua_code_cache` directive is enabled in your nginx
-- config. This approach will be the most efficient as it will avoid re-creating
-- the cluster variable on each request and will preserve the cached state of
-- your load-balancing policy and prepared statements directly in the Lua land.
-------------------------------------------------------------------------------

--
-- my_module.lua
--

local cassandra = require "cassandra"
local Cluster = require "resty.cassandra.cluster"

-- cluster instance as an upvalue
local cluster

local _M = {}

function _M.init_cluster(...)
  cluster = assert(Cluster.new(...))

  -- we also retrieve the cluster's nodes informations early, to avoid
  -- slowing down our first incoming request, which would have triggered
  -- a refresh should this not be done already.
  assert(cluster:refresh())
end

function _M.execute(...)
  return cluster:execute(...)
end

return _M

--
-- nginx.conf
--

http {
  lua_shared_dict cassandra 1m; # shm storing cluster information
  lua_code_cache on;            # ensure the upvalue is preserved beyond a single request

  init_by_lua_block {
    -- will trigger a refresh of the cluster before the first request, but requires
    -- LuaSocket since cosockets are not available in the 'init_by_lua' context.
    local my_module = require "my_module"
    my_module.init_cluster {
      shm = "cassandra", -- defined in http block
      contact_points = {"127.0.0.1", "127.0.0.2"},
      keyspace = "my_keyspace"
    }
  }

  server {
    location / {
      content_by_lua_block {
        local my_module = require "my_module"

        local rows, err = my_module.execute("SELECT * FROM users WHERE id = ? AND name = ?", {
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
