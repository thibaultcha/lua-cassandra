local _M = require('resty.cassandra.policies.reconnection').new_policy('constant')

local type = type

function _M.new(delay)
  if type(delay) ~= 'number' or delay < 1 then
    error('arg #1 delay must be a positive integer', 2)
  end

  local self = _M.super.new()
  self.delay = delay
  return self
end

function _M:reset()

end

function _M:next_delay()
  return self.delay
end

return _M
