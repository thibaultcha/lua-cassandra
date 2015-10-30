--- Represent one Cassandra node
local Object = require "cassandra.classic"
local HostConnection = require "cassandra.host_connection"
local string_find = string.find

--- _HOST
-- @section host

local _HOST = Object:extend()

function _HOST:new(host, port)
  self.address = host..":"..port
  self.casandra_version = nil
  self.datacenter = nil
  self.rack = nil
  self.unhealthy_at = 0
  self.connection = HostConnection(host, port)
end

return _HOST
