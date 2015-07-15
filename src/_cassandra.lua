--------
-- This module allows the creation of sessions and provides shorthand
-- annotations for type encoding and batch statement creation.
-- Depending on how it will be initialized, it supports either the binary
-- protocol v2 or v3:
--
--    require "cassandra" -- binary protocol v3 (Cassandra 2.0.x and 2.1.x)
--    require "cassandra.v2" -- binary procotol v2 (Cassandra 2.0.x)
--
-- Shorthands to give a type to a value in a query:
--
--    session:execute("SELECT * FROM users WHERE id = ?", {
--      cassandra.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93")
--    })
--
-- @module Cassandra

local session = require "cassandra.session"
local batch_mt = require "cassandra.batch"

local _M = {}

function _M:__call(protocol)
  local constants = require("cassandra.constants.constants_"..protocol)
  local Marshaller = require("cassandra.marshallers.marshall_"..protocol)
  local Unmarshaller = require("cassandra.marshallers.unmarshall_"..protocol)
  local Writer = require("cassandra.protocol.writer_"..protocol)
  local Reader = require("cassandra.protocol.reader_"..protocol)

  local marshaller = Marshaller(constants)
  local unmarshaller = Unmarshaller()
  local writer = Writer(marshaller, constants)
  local reader = Reader(unmarshaller, constants)

  local cassandra_t = {
    protocol = protocol,
    writer = writer,
    reader = reader,
    constants = constants,
    marshaller = marshaller,
    unmarshaller = unmarshaller,
    -- extern
    consistency = constants.consistency,
    batch_types = constants.batch_types
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

--- Instanciate a new `Session`.
-- Create a socket with the cosocket API if in Nginx and available, fallback to luasocket otherwise.
-- The instanciated session will communicate using the binary protocol of the current cassandra
-- implementation being required.
-- @return session The created session.
-- @return err     Any `Error` encountered during the socket creation.
function _M:new()
  local tcp, socket_type
  if ngx and ngx.get_phase ~= nil and ngx.get_phase() ~= "init" then
    -- openresty
    tcp = ngx.socket.tcp
    socket_type = "ngx"
  else
    -- fallback to luasocket
    -- It's also a fallback for openresty in the
    -- "init" phase that doesn't support Cosockets
    tcp = require("socket").tcp
    socket_type = "luasocket"
  end

  local socket, err = tcp()
  if not socket then
    return nil, err
  end

  local session_t = {
    socket = socket,
    socket_type = socket_type,
    writer = self.writer,
    reader = self.reader,
    constants = self.constants,
    marshaller = self.marshaller,
    unmarshaller = self.unmarshaller
  }

  return setmetatable(session_t, session)
end

--- Instanciate a `BatchStatement`.
-- The instanciated batch will then provide an ":add()" method to add queries,
-- and can be executed by a session's ":execute()" function.
-- See the related `BatchStatement` module and `batch.lua` example.
-- See http://docs.datastax.com/en/cql/3.1/cql/cql_reference/batch_r.html
-- @param batch_type The type of this batch. Can be one of: 'Logged, Unlogged, Counter'
function _M:BatchStatement(batch_type)
  if not batch_type then
    batch_type = self.constants.batch_types.LOGGED
  end

  return setmetatable({type = batch_type, queries = {}}, batch_mt)
end

return setmetatable({}, _M)
