local _M = require('resty.cassandra.policies.retry').new_policy('simple')

local type = type

function _M.new(max_retries)
  if type(max_retries) ~= 'number' or max_retries < 1 then
    error('arg #1 max_retries must be a positive integer', 2)
  end

  local self = _M.super.new()
  self.max_retries = max_retries
  return self
end

function _M:on_unavailable(request)
  return false
end

function _M:on_read_timeout(request)
  return request.retries < self.max_retries
end

function _M:on_write_timeout(request)
  return request.retries < self.max_retries
end

return _M
