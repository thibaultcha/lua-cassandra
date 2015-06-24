local session = require "cassandra.session"

local _CASS = {}

function _CASS:__call(protocol)
  local cassandra_t = {
    protocol = protocol,
    writer = require("cassandra.protocol.writer_"..protocol),
    reader = require("cassandra.protocol.reader_"..protocol),
    constants = require("cassandra.constants_"..protocol),
    marshaller = require("cassandra.marshallers.marshall_"..protocol),
    unmarshaller = require("cassandra.marshallers.unmarshall_"..protocol)
  }

  return setmetatable(cassandra_t, _CASS)
end

-- Shorthand to create type annotations
-- Ex:
--   session:execute("...", {cassandra.uuid(some_uuid_str)})
function _CASS:__index(key)
  if self.marshaller.TYPES[key] then
    return function(value)
      return {type = key, value = value}
    end
  end

  return _CASS[key]
end

-- Instanciate a new session.
-- Create a socket with the cosocket API if available, fallback on luasocket otherwise.
-- @return `session` The created session.
-- @return `err`     Any error encountered during the socket creation.
function _CASS:new()
  local tcp
  if ngx and ngx.get_phase ~= nil and ngx.get_phase() ~= "init" then
    -- openresty
    tcp = ngx.socket.tcp
  else
    -- fallback to luasocket
    -- It's also a fallback for openresty in the
    -- "init" phase that doesn't support Cosockets
    tcp = require("socket").tcp
  end

  local socket, err = tcp()
  if not socket then
    return nil, err
  end

  local session_t = {
    socket = socket,
    writer = self.writer,
    reader = self.reader,
    constants = self.constants,
    marshaller = self.marshaller,
    unmarshaller = self.unmarshaller
  }

  return setmetatable(session_t, session)
end

return setmetatable({}, _CASS)
