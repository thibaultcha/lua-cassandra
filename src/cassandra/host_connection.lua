--- Represent one socket to connect to a Cassandra node
local Object = require "cassandra.classic"

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
    return nil, err
  end

  return socket, sock_type
end

--- _HOST_CONNECTION
-- @section host_connection

local _HOST_CONNECTION = Object:extend()

function _HOST_CONNECTION:new(host, port)
  local socket, socket_type = new_socket()
  self.host = host
  self.port = port
  self.socket = socket
  self.socket_type = socket_type
end

local function startup()

end

function _HOST_CONNECTION:open()
  local ok, err = self.socket:connect(self.host, self.port)
  return ok == 1, err
end

return _HOST_CONNECTION
