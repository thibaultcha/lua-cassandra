--- Represent a connection from the driver to the cluster and handle events between the two
local Object = require "cassandra.classic"
local Host = require "cassandra.host"
local HostConnection = require "cassandra.host_connection"
local RequestHandler = require "cassandra.request_handler"
local utils = require "cassandra.utils"
local log = require "cassandra.log"
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
    if not port then port = 9042 end -- @TODO add this to some constant
    table_insert(self.hosts, Host(addr, port))
  end

  local any_host, err = RequestHandler.get_first_host(self.hosts)
  if err then
    return nil, err
  end
  -- @TODO get peers info
  -- @TODO get local info
  -- local peers, err
  -- local local_infos, err

  return self.hosts
end

function ControlConnection:get_peers()

end

return ControlConnection
