------------------------------------------
-- Single host module with SSL connections
-- Required modules: LuaSocket, LuaSec
------------------------------------------

local cassandra = require "cassandra"

local client = assert(cassandra.new {
  ssl = true,
  verify = true, -- optionally, verify the server certificate
  cafile = "/path/to/node-certificate.pem" -- optionally, the CA in PEM format
})

assert(client:connect())

--------------------------------------
-- Cluster module with SSL connections
--------------------------------------

http {
  lua_shared_dict cassandra 1m;

  server {
    ...

    location / {
      # this will be used to verify the server certificate
      lua_ssl_trusted_certificate "/path/to/node-certificate.pem";

      content_by_lua_block {
        local Cluster = require "resty.cassandra.cluster"

        local cluster, err = Cluster.new {
          shm = "cassandra", -- defined in http block
          contact_points = {"127.0.0.1", "127.0.0.2"},
          keyspace = "my_keyspace",
          ssl = true,
          verify = true
        }
        if not cluster then
          ngx.log(ngx.ERR, "could not create cluster: ", err)
          ngx.exit(500)
        end

        local ok, err = cluster:refresh() -- automatically called upon first query
        if not ok then
          ngx.log(ngx.ERR, "could not connect to cluster: ", err)
          ngx.exit(500)
        end

        ngx.say("SSL connection: OK")
      }
    }
  }
}
