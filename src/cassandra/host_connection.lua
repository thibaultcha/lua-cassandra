--- Represent one socket to connect to a Cassandra node
local Object = require "cassandra.classic"
local Errors = require "cassandra.errors"
local CONSTS = require "cassandra.consts"
local requests = require "cassandra.requests"
local frame_header = require "cassandra.types.frame_header"
local frame_reader = require "cassandra.frame_reader"
local string_find = string.find

local FrameReader = frame_reader.FrameReader
local FrameHeader = frame_header.FrameHeader

--- Constants
-- @section constants

local SOCKET_TYPES = {
  NGX = "ngx",
  LUASOCKET = "luasocket"
}

--- Utils
-- @section utils

local function new_socket(self)
  local tcp_sock, sock_type

  if ngx and ngx.get_phase ~= nil and ngx.get_phase ~= "init" then
    -- lua-nginx-module
    tcp_sock = ngx.socket.tcp
    sock_type = SOCKET_TYPES.NGX
  else
    -- fallback to luasocket
    tcp_sock = require("socket").tcp
    sock_type = SOCKET_TYPES.LUASOCKET
  end

  local socket, err = tcp_sock()
  if not socket then
    error(err)
  end

  self.socket = socket
  self.socket_type = sock_type
end

--- HostConnection
-- @section host_connection

local HostConnection = Object:extend()

function HostConnection:new(host, port, options)
  self.host = host
  self.port = port
  self.address = host..":"..port
  self.protocol_version = CONSTS.DEFAULT_PROTOCOL_VERSION
  --self.connected = false

  self.log = options.logger
  self.socket_options = options.socket_options

  new_socket(self)
end

function HostConnection:decrease_version()
  self.protocol_version = self.protocol_version - 1
end

--- Socket operations
-- @section socket

function HostConnection:get_reused_times()
  if self.socket_type == SOCKET_TYPES.NGX then
    return self.socket:getreusedtimes()
  end

  -- luasocket
  return 0
end

function HostConnection:close()
  self.log:info("Closing connection to "..self.address..".")
  local res, err = self.socket:close()
  if res ~= 1 then
    self.log:err("Could not close socket for connection to "..self.address..". "..err)
    return false, err
  else
    --self.connected = false
    return true
  end
end

function HostConnection:set_timeout(timeout)
  if self.socket_type == SOCKET_TYPES.LUASOCKET then
    -- value is in seconds
    timeout = timeout / 1000
  end

  self.socket:settimeout(timeout)
end

local function send_and_receive(self, request)
  -- Send frame
  local bytes_sent, err = self.socket:send(request:get_full_frame())
  if bytes_sent == nil then
    return nil, err
  end

  -- Receive frame version byte
  local frame_version_byte, err = self.socket:receive(1)
  if frame_version_byte == nil then
    return nil, err
  end

  local n_bytes_to_receive = FrameHeader.size_from_byte(frame_version_byte) - 1

  -- Receive frame header
  local header_bytes, err = self.socket:receive(n_bytes_to_receive)
  if header_bytes == nil then
    return nil, err
  end

  local frameHeader = FrameHeader.from_raw_bytes(frame_version_byte, header_bytes)

  -- Receive frame body
  local body_bytes
  if frameHeader.body_length > 0 then
    body_bytes, err = self.socket:receive(frameHeader.body_length)
    if body_bytes == nil then
      return nil, err
    end
  end

  return FrameReader(frameHeader, body_bytes)
end

function HostConnection:send(request)
  request:set_version(self.protocol_version)

  self:set_timeout(self.socket_options.read_timeout)

  local frameReader, err = send_and_receive(self, request)
  if err then
    if err == "timeout" then
      return nil, Errors.TimeoutError(self.address)
    else
      return nil, Errors.SocketError(self.address, err)
    end
  end

  --self:close()

  -- result, cql_error
  return frameReader:parse()
end

--- Determine the protocol version to use and send the STARTUP request
local function startup(self)
  self.log:info("Startup request. Trying to use protocol v"..self.protocol_version)

  local startup_req = requests.StartupRequest()
  return self.send(self, startup_req)
end

function HostConnection:open()
  --if self.connected then return true end

  self:set_timeout(self.socket_options.connect_timeout)

  self.log:info("Connecting to "..self.address)
  local ok, err = self.socket:connect(self.host, self.port)
  if ok ~= 1 then
    self.log:info("Could not connect to "..self.address..". "..err)
    return false, err
  end
  self.log:info("Socket connected to "..self.address)

  -- Startup request if this socket has never been connected to it
  if self:get_reused_times() > 0 then
    return true
  end

  local res, err = startup(self)
  if err then
    self.log:info("Startup request failed. "..err)
    -- Check for incorrect protocol version
    if err and err.code == frame_reader.errors.PROTOCOL then
      if string_find(err.message, "Invalid or unsupported protocol version:", nil, true) then
        self:close()
        self:decrease_version()
        if self.protocol_version < CONSTS.MIN_PROTOCOL_VERSION then
          self.log:err("Connection could not find a supported protocol version.")
        else
          self.log:info("Decreasing protocol version to v"..self.protocol_version)
          return self:open()
        end
      end
    end

    return false, err
  elseif res.ready then
    --self.connected = true
    self.log:info("Host at "..self.address.." is ready with protocol v"..self.protocol_version)
    return true
  end
end

return HostConnection
