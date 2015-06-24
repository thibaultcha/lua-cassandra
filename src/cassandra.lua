local utils = require "cassandra.utils"

local _M = {
  CQL_VERSION = "3.0.0"
}

-- Shorthand to create type annotations
-- Ex:
--   session:execute("...", {session.uuid(some_uuid_str)})
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
function _M.new()
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

  local session = {
    socket = socket,
    writer = require("cassandra.protocol.writer_v2"),
    reader = require("cassandra.protocol.reader_v2"),
    constants = require("cassandra.constants_v2"),
    marshaller = require("cassandra.marshallers.marshall_v2"),
    unmarshaller = require("cassandra.marshallers.unmarshall_v2")
  }

  return setmetatable(session, _M)
end

local function send_frame_and_get_response(self, op_code, frame_body, tracing)
  local bytes, response, err
  local frame = self.writer.build_frame(self, op_code, frame_body, tracing)
  bytes, err = self.socket:send(frame)
  if not bytes then
    return nil, string.format("Failed to send frame to %s: %s", self.host, err)
  end
  response, err = self.reader.reveive_frame(self)
  if not response then
    return nil, err
  end
  return response
end

local function startup(self)
  local frame_body = self.marshaller.string_map_representation({["CQL_VERSION"]=_M.CQL_VERSION})
  local response, err = send_frame_and_get_response(self, self.constants.op_codes.STARTUP, frame_body)
  if not response then
    return false, err
  end
  if response.op_code ~= self.constants.op_codes.READY then
    return false, "server is not ready"
  end
  return true
end

-- Connect a session to a node coordinator.
-- @throw Any error due to a wrong usage of the driver.
-- @return `connected` A boolean indicating the success of the connection.
-- @return `err`       Any server/client error encountered during the connection.
function _M:connect(contact_points, port)
  if port == nil then port = 9042 end
  if contact_points == nil then
    error("no contact points provided", 2)
  elseif type(contact_points) == "table" then
    -- shuffle the contact points so we don't try  to connect always on the same order,
    -- avoiding pressure on the same node cordinator.
    utils.shuffle_array(contact_points)
  else
    contact_points = {contact_points}
  end

  if not self.socket then
    error("session does not have a socket, create a new session first", 2)
  end

  local ok, err
  for _, contact_point in ipairs(contact_points) do
    -- Extract port if string is of the form "host:port"
    local host, host_port = utils.split_by_colon(contact_point)
    if not host_port then -- Default port is the one given as parameter
      host_port = port
    end
    ok, err = self.socket:connect(host, host_port)
    if ok then
      self.host = host
      self.port = host_port
      break
    end
  end

  if not ok then
    return false, err
  end

  if not self.ready then
    self.ready, err = startup(self)
    if not self.ready then
      return false, err
    end
  end

  return true
end

-- Close a session.
-- Wrapper around the cosocket (or luasocket) `:close()`.
-- @throw Any error due to a wrong usage of the driver.
-- @see http://wiki.nginx.org/HttpLuaModule#tcpsock:close
-- @see http://w3.impa.br/~diego/software/luasocketp.html#close
function _M:close()
  if not self.socket then
    error("session does not have a socket, create a new session first", 2)
  end
  return self.socket:close()
end

-- Default query options.
-- @see `:execute()`
local default_options = {
  page_size = 5000,
  auto_paging = false,
  tracing = false
}

local function page_iterator(session, operation, args, options)
  local page = 0
  local rows, err
  return function(operation, previous_rows)
    if previous_rows and previous_rows.meta.has_more_pages == false then
      return nil -- End iteration after error
    end

    rows, err = session:execute(operation, args, {
      page_size = options.page_size,
      paging_state =  previous_rows and previous_rows.meta.paging_state
    })

    -- If we have some results, increment the page
    if rows ~= nil and #rows > 0 then
      page = page + 1
    else
      if err then
        -- Just expose the error with 1 last iteration
        return {meta={has_more_pages=false}}, err, page
      elseif rows.meta.has_more_pages == false then
        return nil -- End of the iteration
      end
    end

    return rows, err, page
  end, operation, nil
end

-- Execute an operation (string query, prepared statement, batch statement).
-- Will send the query, parse the response and return it.
-- @param  `operation` The operation to execute.
-- @param  `args`      (Optional) An array of arguments to bind to the operation.
-- @param  `options`   (Optional) A table of options to assign to this query.
-- @return `response`  The parsed response from Cassandra.
-- @return `err`       Any error encountered during the execution.
function _M:execute(operation, args, options)
  if not options then options = {} end
  -- Default options
  if not options.consistency_level then
    options.consistency_level = self.constants.consistency.ONE
  end
  for k in pairs(default_options) do
    if options[k] == nil then options[k] = default_options[k] end
  end

  if options.auto_paging then
    return page_iterator(self, operation, args, options)
  end

  local frame_body, op_code = self.writer.build_body(self, operation, args, options)
  local response, err = send_frame_and_get_response(self, op_code, frame_body, options.tracing)
  if not response then
    return nil, err
  end

  return self.reader.parse_response(self, response)
end

function _M:set_keyspace(keyspace)
  return self:execute(string.format("USE \"%s\"", keyspace))
end

function _M:prepare(query, tracing)
  local frame_body = self.marshaller.long_string_representation(query)
  local response, err = send_frame_and_get_response(self, self.constants.op_codes.PREPARE, frame_body, tracing)
  if not response then
    return nil, err
  end

  return self.reader.parse_response(self, response)
end

return _M
