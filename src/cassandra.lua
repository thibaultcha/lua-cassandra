local utils = require "cassandra.utils"

local _M = {
  CQL_VERSION = "3.0.0"
}
_M.__index = _M

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
  local frame = self.writer.build_frame(self, op_code, frame_body, tracing)
  local bytes, err = self.socket:send(frame)
  if not bytes then
    return nil, string.format("Failed to send frame to %s: %s", self.host, err)
  end
  local response, err = self.reader.read_frame(self)
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
    error("session does not have a socket, create a new session first.", 2)
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

function _M:execute(operation, args, options)
  if not options then options = {} end
  -- Default options
  if not operation.consistency_level then
    options.consistency_level = self.constants.consistency.ONE
  end

  local frame_body, op_code = self.writer.build_body(self, operation, args, options)
  local response, err = send_frame_and_get_response(self, op_code, frame_body)
  if not response then
    return nil, err
  elseif response.op_code ~= self.constants.op_codes.RESULT then
    return nil, "result expected"
  end

  return self.reader.parse_response(self, response)
end

return _M
