local Object = require "cassandra.classic"
local Errors = require "cassandra.errors"

--- RequestHandler
-- @section request_handler

local RequestHandler = Object:extend()

function RequestHandler:new(request, hosts, client_options)
  self.request = request
  self.hosts = hosts

  self.connection = nil
  self.load_balancing_policy = client_options.policies.load_balancing
  self.retry_policy = nil -- @TODO
  self.log = client_options.logger
end

function RequestHandler:get_next_connection()
  local errors = {}
  local iter = self.load_balancing_policy:iterator()

  for _, host in iter(self.hosts) do
    if host:can_be_considered_up() then
      local connected, err = host:open()
      if connected then
        return host.connection
      else
        host:set_down()
        errors[host.address] = err
      end
    else
      errors[host.address] = "Host considered DOWN"
    end
  end

  return nil, Errors.NoHostAvailableError(errors)
end

function RequestHandler:send()
  local connection, err = self:get_next_connection()
  if not connection then
    return nil, err
  end

  self.log:info("Acquired connection through load balancing policy: "..connection.address)

  return connection:send(self.request)
end

-- Get the first connection from the available one with no regards for the load balancing policy
function RequestHandler.get_first_host(hosts)
  local errors = {}
  for _, host in pairs(hosts) do
    local connected, err = host:open()
    if not connected then
      errors[host.address] = err
    else
      return host
    end
  end

  return nil, Errors.NoHostAvailableError(errors)
end

return RequestHandler
