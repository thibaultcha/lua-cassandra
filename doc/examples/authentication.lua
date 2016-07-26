--------------------------------
-- Single host module, plain Lua
--------------------------------

local cassandra = require "cassandra"

local peer = assert(cassandra.new {
  auth = cassandra.auth_providers.plain_text("cassandra", "cassandra")
})

assert(peer:connect())

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
          auth = cassandra.auth_providers.plain_text("cassandra", "cassandra")
        }
        if not cluster then
          ngx.log(ngx.ERR, "could not create cluster: ", err)
          ngx.exit(500)
        end
      }
    }
  }
}
