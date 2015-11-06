--- Responsible for a cluster of nodes
local Object = require "cassandra.classic"
local client_options = require "cassandra.client_options"
local ControlConnection = require "cassandra.control_connection"
local Logger = require "cassandra.logger"
local Requests = require "cassandra.requests"
local RequestHandler = require "cassandra.request_handler"

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
  self.log = options.logger

  self.controlConnection = ControlConnection(options)
end

local function _connect(self)
  if self.connected then return end

  local err
  self.hosts, err = self.controlConnection:init()
  if err then
    return err
  end

  --local inspect = require "inspect"
  --print(inspect(self.hosts))

  self.connected = true
end

Client._connect = _connect

function Client:execute(query)
  local err = _connect(self)
  if err then
    return nil, err
  end

  local query_request = Requests.QueryRequest(query)
  local handler = RequestHandler(query_request, self.hosts, self.options)
  return handler:send()
end

--- Close connection to the cluster.
-- Close all connections to all hosts and forget about them.
-- @return err An error from socket:close() if any, nil otherwise.
function Client:shutdown()
  self.log:info("Shutting down")
  if not self.connected or self.hosts == nil then
    return
  end

  for _, host in pairs(self.hosts) do
    local closed, err = host:shutdown()
    if not closed then
      return err
    end
  end

  self.hosts = {}
  self.connected = false
end

return Client
