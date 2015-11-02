--- Represent one Cassandra node
local Object = require "cassandra.classic"
local HostConnection = require "cassandra.host_connection"
local string_find = string.find

--- Host
-- @section host

local Host = Object:extend()

function Host:new(address, port)
  self.address = address..":"..port
  self.casandra_version = nil
  self.datacenter = nil
  self.rack = nil
  self.unhealthy_at = 0
  self.connection = HostConnection(address, port)
end

return Host
