--------
-- lua-cassandra supports plain text authentication, such as PasswordAuthenticator
-- @see http://docs.datastax.com/en/cassandra/2.1/cassandra/security/security_config_native_authenticate_t.html

local cassandra = require "cassandra"

local session, err = cassandra.spawn_session {
  shm = "cassandra",
  contact_points = {"127.0.0.1", "127.0.0.2", "127.0.0.3"},
  auth = cassandra.auth.PlainTextProvider("username", "password")
}
assert(err == nil)

-- ...

session:shutdown()
