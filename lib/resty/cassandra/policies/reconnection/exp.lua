local _M = require('resty.cassandra.policies.reconnection').new_policy('exponential')

local type = type
local min = math.min
local pow = math.pow

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
