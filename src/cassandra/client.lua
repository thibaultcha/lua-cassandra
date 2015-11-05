--- Responsible for a cluster of nodes
local Object = require "cassandra.classic"
local client_options = require "cassandra.client_options"
local ControlConnection = require "cassandra.control_connection"
local Logger = require "cassandra.logger"

--- CLIENT
-- @section client

local Client = Object:extend()

function Client:new(options)
  options = client_options.parse(options)
  options.logger = Logger(options.print_log_level)

  self.options = options
  self.keyspace = options.keyspace
  self.hosts = {}
  self.connected = false

  self.controlConnection = ControlConnection(options)
end

local function _connect(self)
  if self.connected then return end

  local err
  self.hosts, err = self.controlConnection:init()
  if err then
    return err
  end

  local inspect = require "inspect"
  --print(inspect(self.hosts))

  self.connected = true
end

function Client:execute()
  local err = _connect(self)
  if err then
    return err
  end
end

return Client
