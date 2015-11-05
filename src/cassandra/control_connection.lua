--- Represent a connection from the driver to the cluster and handle events between the two
local CONSTS = require "cassandra.consts"
local Object = require "cassandra.classic"
local Host = require "cassandra.host"
local HostConnection = require "cassandra.host_connection"
local RequestHandler = require "cassandra.request_handler"
local requests = require "cassandra.requests"
local table_insert = table.insert

--- Constants
-- @section constants

local SELECT_PEERS_QUERY = "SELECT peer,data_center,rack,rpc_address,release_version FROM system.peers"
local SELECT_LOCAL_QUERY = "SELECT data_center,rack,rpc_address,release_version FROM system.local WHERE key='local'"

--- CONTROL_CONNECTION
-- @section control_connection

local ControlConnection = Object:extend()

function ControlConnection:new(options)
  self.hosts = {}
  self.log = options.logger
  self.options = options
end

function ControlConnection:init()
  local contact_points = {}
  for _, address in ipairs(self.options.contact_points) do
    -- Extract port if string is of the form "host:port"
    contact_points[address] = Host(address, self.options)
  end

  local err

  local host, err = RequestHandler.get_first_host(contact_points)
  if err then
    return nil, err
  end

  err = self:refresh_hosts(host)
  if err then
    return nil, err
  end

  return self.hosts
end

function ControlConnection:refresh_hosts(host)
  self.log:info("Refreshing local and peers info")
  local err

  err = self:get_local(host)
  if err then
    return err
  end

  err = self:get_peers(host)
  if err then
    return err
  end
end

function ControlConnection:get_local(host)
  local local_query = requests.QueryRequest(SELECT_LOCAL_QUERY)
  local rows, err = host.connection:send(local_query)
  if err then
    return err
  end

  local row = rows[1]
  local local_host = self.hosts[host.address]
  if not local_host then
    local_host = Host(host.address, self.options)
  end

  local_host.datacenter = row["data_center"]
  local_host.rack = row["rack"]
  local_host.cassandra_version = row["release_version"]
  local_host.connection.protocol_version = host.connection.protocol_version

  self.hosts[host.address] = local_host
  self.log:info("Local info retrieved")
end

function ControlConnection:get_peers(host)
  local peers_query = requests.QueryRequest(SELECT_PEERS_QUERY)
  local rows, err = host.connection:send(peers_query)
  if err then
    return err
  end

  for _, row in ipairs(rows) do
    local address = self.options.policies.address_resolution(row["rpc_address"])
    local new_host = self.hosts[address]
    if new_host == nil then
      new_host = Host(address, self.options)
      self.log:info("Adding host "..new_host.address)
    end

    new_host.datacenter = row["data_center"]
    new_host.rack = row["rack"]
    new_host.cassandra_version = row["release_version"]
    new_host.connection.protocol_version = host.connection.protocol_version

    self.hosts[address] = new_host
  end

  self.log:info("Peers info retrieved")
end

function ControlConnection:add_hosts(rows, host_connection)

end

return ControlConnection
