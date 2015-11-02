--- Represent one socket to connect to a Cassandra node
local Object = require "cassandra.classic"
local CONSTS = require "cassandra.consts"
local log = require "cassandra.log"
local requests = require "cassandra.requests"
local frame_header = require "cassandra.types.frame_header"
local FrameReader = require "cassandra.frame_reader"
local FrameHeader = frame_header.FrameHeader

--- Constants
-- @section constants

local SOCKET_TYPES = {
  NGX = "ngx",
  LUASOCKET = "luasocket"
}

--- Utils
-- @section utils

local function new_socket()
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
    return nil, nil, err
  end

  return socket, sock_type
end

--- HostConnection
-- @section host_connection

local HostConnection = Object:extend()

function HostConnection:new(address, port)
  local socket, socket_type, err = new_socket()
  if err then
    error(err)
  end
  self.address = address
  self.port = port
  self.socket = socket
  self.socket_type = socket_type
  self.protocol_version = CONSTS.DEFAULT_PROTOCOL_VERSION
end

--- Socket operations
-- @section socket

local function send_and_receive(self, request)
  request.version = self.protocol_version

  -- Send frame
  local bytes_sent, err = self.socket:send(request:get_full_frame())
  if bytes_sent == nil then
    return nil, err
  end

  local n_bytes_to_receive
  if self.protocol_version < 3 then
    n_bytes_to_receive = 8
  else
    n_bytes_to_receive = 9
  end

  -- Receive frame header
  local header_bytes, err = self.socket:receive(n_bytes_to_receive)
  if header_bytes == nil then
    return nil, err
  end
  local frameHeader = FrameHeader.from_raw_bytes(header_bytes)

  -- Receive frame body
  local body_bytes, err = self.socket:receive(frameHeader.body_length)
  if body_bytes == nil then
    return nil, err
  end
  local frameReader = FrameReader(frameHeader, body_bytes)

  return frameReader:read()
end

local function startup(self)
  log.debug("Startup request. Trying to use protocol: "..self.protocol_version)

  local startup_req = requests.StartupRequest()
  return send_and_receive(self, startup_req)
end

function HostConnection:open()
  local address = self.address..":"..self.port
  log.debug("Connecting to "..address)
  local ok, err = self.socket:connect(self.address, self.port)
  if ok ~= 1 then
    log.debug("Could not connect to "..address)
    return false, err
  end
  log.debug("Socket connected to "..self.address)

  local res, err = startup(self)
  if err then
    log.debug("Startup request failed. "..err)
    return false, err
  elseif res.ready then
    log.debug("Host at "..address.." is ready")
    return true
  end
end

return HostConnection
