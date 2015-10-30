--- Responsible for a cluster of nodes
local Object = require "cassandra.classic"
local client_options = require "cassandra.client_options"
local ControlConnection = require "cassandra.control_connection"

--- CLIENT
-- @section client

local _CLIENT = Object:extend()

function _CLIENT:new(options)
  options = client_options.parse(options)
  self.keyspace = options.keyspace
  self.hosts = {}
  self.connected = false
  self.controlConnection = ControlConnection({contact_points = options.contact_points})
end

local function _connect(self)
  if self.connected then return end

  local err
  self.hosts, err = self.controlConnection:init()
  local inspect = require "inspect"
  print("Hosts: ", inspect(self.hosts))
  if err then
    return err
  end

  self.connected = true
end

function _CLIENT:execute()
  local err = _connect(self)
  if err then
    return err
  end
end

return _CLIENT
