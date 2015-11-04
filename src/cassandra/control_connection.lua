--- Represent a connection from the driver to the cluster and handle events between the two
local CONSTS = require "cassandra.consts"
local Object = require "cassandra.classic"
local Host = require "cassandra.host"
local HostConnection = require "cassandra.host_connection"
local RequestHandler = require "cassandra.request_handler"
local utils = require "cassandra.utils"
local log = require "cassandra.log"
local requests = require "cassandra.requests"
local table_insert = table.insert

--- Constants
-- @section constants

local SELECT_PEERS_QUERY = "SELECT peer,data_center,rack,tokens,rpc_address,release_version FROM system.peers"
local SELECT_LOCAL_QUERY = "SELECT * FROM system.local WHERE key='local'"

--- CONTROL_CONNECTION
-- @section control_connection

local ControlConnection = Object:extend()

function ControlConnection:new(options)
  -- @TODO check attributes are valid (contact points, etc...)
  self.hosts = {}
  self.contact_points = options.contact_points
end

function ControlConnection:init()
  for _, contact_point in ipairs(self.contact_points) do
    -- Extract port if string is of the form "host:port"
    local addr, port = utils.split_by_colon(contact_point)
    if not port then port = CONSTS.DEFAULT_CQL_PORT end
    table_insert(self.hosts, Host(addr, port))
  end

  local any_host, err = RequestHandler.get_first_host(self.hosts)
  if err then
    return nil, err
  end

  local err = self:refresh_hosts(any_host)

  -- @TODO get peers info
  -- @TODO get local info
  -- local peers, err
  -- local local_infos, err

  return self.hosts
end

function ControlConnection:refresh_hosts(host)
  log.debug("Refreshing local and peers info")
  return self:get_peers(host)
end

function ControlConnection:get_peers(host)
  local peers_query = requests.QueryRequest(SELECT_PEERS_QUERY)
  local result, err = host.connection:send(peers_query)
  if err then
    return err
  end

  local inspect = require "inspect"
  print("Peers result: "..inspect(result))
end

return ControlConnection
