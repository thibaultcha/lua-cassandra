--------
-- Example of SSL enabled connection using LuaSocket in plain Lua
-- Required modules: LuaSocket, LuaSec
-- @see http://docs.datastax.com/en/cassandra/2.1/cassandra/security/secureSslEncryptionTOC.html

local cassandra = require "cassandra"

local session, err = cassandra.spawn_session {
  shm = "cassandra",
  contact_points = {"127.0.0.1", "127.0.0.2", "127.0.0.3"},
  ssl_options = {
    enabled = true,
    verify = true, -- optionally, verify the server certificate
    ca = "/path/to/certificate-authority.pem" -- optionally, the CA file to verify the server certificate
}
assert(err == nil)

-- ...

session:shutdown()

--------
-- Example of SSL enabled connection from ngx_lua

http {

  lua_shared_dict cassandra 1m;

  server {
    ...

    location / {

      # this CA will be used to verify the server certificate
      lua_ssl_trusted_certificate "/path/to/certificate-authority.pem";

      content_by_lua_block {
        local cassandra = require "cassandra"

        local session, err = cassandra.spawn_session {
          shm = "cassandra",
          contact_points = {"127.0.0.1", "127.0.0.2", "127.0.0.3"},
          ssl_options = {
            enabled = true,
            verify = true -- optionally, verify the server certificate
          }
        }
        if err then
          ngx.log(ngx.ERR, err)
          ngx.exit(500)
        end

        -- ...

        session:set_keep_alive()
      }
    }
  }
}

