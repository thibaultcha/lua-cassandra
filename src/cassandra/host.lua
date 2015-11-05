--- Represent one Cassandra node
local Object = require "cassandra.classic"
local utils = require "cassandra.utils.string"
local HostConnection = require "cassandra.host_connection"
local string_find = string.find

--- Host
-- @section host

local Host = Object:extend()

function Host:new(address, options)
  local host, port = utils.split_by_colon(address)
  if not port then port = options.protocol_options.default_port end

  self.address = address

  self.cassandra_version = nil
  self.datacenter = nil
  self.rack = nil

  self.unhealthy_at = 0
  self.connection = HostConnection(host, port, {logger = options.logger})
end

function Host:shutdown()
  return self.connection:close()
end

return Host
