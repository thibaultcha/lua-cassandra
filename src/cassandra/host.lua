--- Represent one Cassandra node
local Object = require "cassandra.classic"
local time_utils = require "cassandra.utils.time"
local string_utils = require "cassandra.utils.string"
local HostConnection = require "cassandra.host_connection"

--- Host
-- @section host

local Host = Object:extend()

function Host:new(address, options)
  local host, port = string_utils.split_by_colon(address)
  if not port then port = options.protocol_options.default_port end

  self.address = address
  self.cassandra_version = nil
  self.datacenter = nil
  self.rack = nil

  self.unhealthy_at = 0
  self.reconnection_delay = 5 -- seconds

  self.log = options.logger
  self.connection = HostConnection(host, port, {logger = options.logger})
end

function Host:set_down()
  if not self:is_up() then
    return
  end
  self.log:warn("Setting host "..self.address.." as DOWN")
  self.unhealthy_at = time_utils.get_time()
  self:shutdown()
end

function Host:set_up()
  if self:is_up() then
    return
  end
  self.log:info("Setting host "..self.address.." as UP")
  self.unhealthy_at = 0
end

function Host:is_up()
  return self.unhealthy_at == 0
end

function Host:can_be_considered_up()
  return self:is_up() or (time_utils.get_time() - self.unhealthy_at > self.reconnection_delay)
end

function Host:open()
  if not self:is_up() then
    self.log:err("RETRYING OPENING "..self.address)
  end
  return self.connection:open()
end

function Host:shutdown()
  return self.connection:close()
end

return Host
