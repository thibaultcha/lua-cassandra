--- Represent one socket to connect to a Cassandra node
local Object = require "cassandra.classic"
local CONSTS = require "cassandra.consts"
local log = require "cassandra.log"
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

function HostConnection:new(address, port)
  self.address = address
  self.port = port
  self.protocol_version = CONSTS.DEFAULT_PROTOCOL_VERSION
end

function HostConnection:decrease_version()
  self.protocol_version = self.protocol_version - 1
  if self.protocol_version < CONSTS.MIN_PROTOCOL_VERSION then
    error("minimum protocol version supported: ", CONSTS.MIN_PROTOCOL_VERSION)
  end
end

--- Socket operations
-- @section socket

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
  print("BODY BYTES: "..frameHeader.body_length)
  print("OP_CODE: "..frameHeader.op_code)

  -- Receive frame body
  local body_bytes
  if frameHeader.body_length > 0 then
    body_bytes, err = self.socket:receive(frameHeader.body_length)
    if body_bytes == nil then
      return nil, err
    end
  end

  local frameReader = FrameReader(frameHeader, body_bytes)

  return frameReader:parse()
end


function HostConnection:send(request)
  request:set_version(self.protocol_version)
  return send_and_receive(self, request)
end

function HostConnection:close()
  local res, err = self.socket:close()
  if err then
    log.err("Could not close socket for connection to "..self.address..":"..self.port..". ", err)
  end
  return res == 1
end

--- Determine the protocol version to use and send the STARTUP request
local function startup(self)
  log.debug("Startup request. Trying to use protocol v"..self.protocol_version)

  local startup_req = requests.StartupRequest()
  return self.send(self, startup_req)
end

function HostConnection:open()
  local address = self.address..":"..self.port
  new_socket(self)

  log.debug("Connecting to ", address)
  local ok, err = self.socket:connect(self.address, self.port)
  if ok ~= 1 then
    log.debug("Could not connect to "..address, err)
    return false, err
  end
  log.debug("Socket connected to ", address)

  local res, err = startup(self)
  if err then
    log.debug("Startup request failed. ", err)
    -- Check for incorrect protocol version
    if err and err.code == frame_reader.errors.PROTOCOL then
      if string_find(err.message, "Invalid or unsupported protocol version:", nil, true) then
        self:close()
        self:decrease_version()
        log.debug("Decreasing protocol version to v"..self.protocol_version)
        return self:open()
      end
    end

    return false, err
  elseif res.ready then
    log.debug("Host at "..address.." is ready with protocol v"..self.protocol_version)
    return true
  end
end

return HostConnection
