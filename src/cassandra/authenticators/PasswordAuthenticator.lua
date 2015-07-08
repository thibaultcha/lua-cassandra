--- The client authenticator for the Cassandra `PasswordAuthenticator` IAuthenticator.
-- To be instanciated with a user/password couple and given to a `Session` when
-- connecting it. See the related `authentication.lua` example.
-- @see http://docs.datastax.com/en/cassandra/1.2/cassandra/security/security_config_native_authenticate_t.html
-- @usage local auth = PasswordAuthenticator("user", "password")
-- @module PasswordAuthenticator

local Object = require "cassandra.classic"
local marshaller = require "cassandra.marshallers.marshall_v2"

local _M = Object:extend()

-- The IAuthenticator class name for which this client authenticator works
_M.class_name = "org.apache.cassandra.auth.PasswordAuthenticator"

function _M:new(user, password)
  if user == nil then
    error("no user provided for PasswordAuthenticator")
  elseif password == nil then
    error("no password provided for PasswordAuthenticator")
  end

  self.user = user
  self.password = password
end

function _M:build_body()
  return marshaller.bytes_representation(string.format("\0%s\0%s", self.user, self.password))
end

function _M:authenticate(session)
  local frame_body = self:build_body()
  local response, socket_err = session:send_frame_and_get_response(session.constants.op_codes.AUTH_RESPONSE, frame_body)
  if socket_err then
    return false, socket_err
  end

  if response.op_code == session.constants.op_codes.AUTH_SUCCESS then
    return true
  else
    local parsed_error = session.reader:read_error(response.buffer)
    return false, parsed_error
  end
end

return _M
