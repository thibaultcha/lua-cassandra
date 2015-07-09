--------
-- Example of SSL enabled connection using luasocket

local cassandra = require "cassandra"
local PasswordAuthenticator = require "cassandra.authenticators.PasswordAuthenticator"

local session = cassandra:new()
local auth = PasswordAuthenticator("cassandra", "cassandra")

local ok, err = session:connect({"x.x.x.x", "y.y.y.y"}, nil, {
  authenticator = auth,
  ssl = true,
  ssl_verify = true,
  ca_file = "/path/to/your/ca-certificate.pem"
})
if not ok then
  print(err.message)
end

local res, err = session:execute("SELECT * FROM system_auth.users")

--------
-- Example of SSL enabled connection from nginx

worker_processes 1;
error_log logs/error.log;
events {
  worker_connections 1024;
}
http {
  server {
    listen 8080;
    location / {
      lua_ssl_trusted_certificate "/path/to/your/ca-certificate.pem";
      default_type text/html;
      content_by_lua '
        local cassandra = require "cassandra"
        local PasswordAuthenticator = require "cassandra.authenticators.PasswordAuthenticator"

        local session = cassandra:new()
        local auth = PasswordAuthenticator("cassandra", "cassandra")

        local ok, err = session:connect({"x.x.x.x", "y.y.y.y"}, nil, {
          authenticator = auth,
          ssl = true,
          ssl_verify = true
        })
        if not ok then
          ngx.log(ngx.ERR, err.message)
        end

        local res, err = session:execute("SELECT * FROM system_auth.users")
      ';
    }
  }
}

