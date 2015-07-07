--------
-- Example with the `PasswordAuthenticator` IAuthenticator
-- @see http://docs.datastax.com/en/cassandra/1.2/cassandra/security/security_config_native_authenticate_t.html

local cassandra = require "cassandra"
local PasswordAuthenticator = require "cassandra.authenticators.PasswordAuthenticator"

local auth = PasswordAuthenticator("user", "password")
local session = cassandra:new()

local ok, err = session:connect("127.0.0.1", nil, auth)

-- Authenticated
local rows, err = session:execute("SELECT * FROM system_auth.users")
