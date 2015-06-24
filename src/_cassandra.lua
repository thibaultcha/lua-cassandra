local session = require "cassandra.session"

local _M = {}

function _M:__call(protocol)
  local cassandra_t = {
    protocol = protocol,
    writer = require("cassandra.protocol.writer_"..protocol),
    reader = require("cassandra.protocol.reader_"..protocol),
    constants = require("cassandra.constants.constants_"..protocol),
    marshaller = require("cassandra.marshallers.marshall_"..protocol),
    unmarshaller = require("cassandra.marshallers.unmarshall_"..protocol)
  }

  return setmetatable(cassandra_t, _M)
end

-- Shorthand to create type annotations
-- Ex:
--   session:execute("...", {cassandra.uuid(some_uuid_str)})
function _M:__index(key)
  if self.marshaller.TYPES[key] then
    return function(value)
      return {type = key, value = value}
    end
  end

  return _M[key]
end

-- Instanciate a new session.
-- Create a socket with the cosocket API if available, fallback on luasocket otherwise.
-- @return `session` The created session.
-- @return `err`     Any error encountered during the socket creation.
function _M:new()
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

local batch_statement_mt = {
  __index = {
    add = function(self, query, args)
      table.insert(self.queries, {query = query, args = args})
    end,
    is_batch_statement = true
  }
}

function _M:BatchStatement(batch_type)
  if not batch_type then
    batch_type = self.constants.batch_types.LOGGED
  end

  return setmetatable({type = batch_type, queries = {}}, batch_statement_mt)
end

return setmetatable({}, _M)
