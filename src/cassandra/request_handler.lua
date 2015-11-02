local Object = require "cassandra.classic"
local Errors = require "cassandra.errors"
local ipairs = ipairs

--- RequestHandler
-- @section request_handler

local RequestHandler = Object:extend()

function RequestHandler:mew(options)
  self.loadBalancingPolicy = nil -- @TODO
  self.retryPolicy = nil -- @TODO
  self.request = options.request
  self.host = options.host
end

-- Get the first connection from the available one with no regards for the load balancing policy
function RequestHandler.get_first_host(hosts)
  local errors = {}
  for _, host in ipairs(hosts) do
    local connected, err = host.connection:open()
    if not connected then
      errors[host.address] = err
    else
      return host
    end
  end

  return nil, Errors.NoHostAvailableError(errors)
end

return RequestHandler
