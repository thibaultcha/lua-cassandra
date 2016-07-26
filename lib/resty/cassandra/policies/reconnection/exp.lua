--- Exponential reconnection policy.
-- This policy will allow the cluster module to retry an unhealthy node after
-- an exponentially growing delay.
-- @module resty.cassandra.policies.reconnection.exp
-- @author thibaultcha

local _M = require('resty.cassandra.policies.reconnection').new_policy('exponential')

local type = type
local min = math.min
local pow = math.pow

--- Create an exponential reconnection policy.
-- Instanciates an exponential reconnection policy for
-- `resty.cassandra.cluster`
--
-- @usage
-- local Cluster = require "resty.cassandra.cluster"
-- local exp_reconn = require "resty.cassandra.policies.reconnection.exp"
--
-- local policy = exp_reconn.new(1000, 60000)
-- local cluster = assert(Cluster.new {
--   reconn_policy = policy
-- })
--
-- @param[type=number] base_delay The original, minimum delay for the first
-- reconnection attempt. Futher attempts will grow exponentially from this
-- delay.
-- @param[type=number] max_delay The maximum allowed delay for a reconnection
-- attempt.
-- @treturn table `policy`: An exponential reconnection policy.
function _M.new(base, max)
  if type(base) ~= 'number' or base < 1 then
    error('arg #1 base must be a positive integer', 2)
  elseif type(max) ~= 'number' or max < 1 then
    error('arg #2 max must be a positive integer', 2)
  end

  local self = _M.super.new()
  self.base = base
  self.max = max
  self.delays = {}
  return self
end

function _M:reset(host)
  if self.delays[host] then
    self.delays[host] = nil
  end
end

function _M:next_delay(host)
  local delays = self.delays
  local idx = delays[host] or 1

  delays[host] = idx + 1

  return min(pow(idx, 2) * self.base, self.max)
end

return _M
