local decisions = {
  throw = 0,
  retry = 1
}

local SimpleRetry = {}
SimpleRetry.__index = SimpleRetry

function SimpleRetry.new(max_retries)
  return setmetatable({max_retries = max_retries}, SimpleRetry)
end

function SimpleRetry:on_unavailable(request_infos)
  return decisions.throw
end

function SimpleRetry:on_read_timeout(request_infos)
  if request_infos.n_retries > self.max_retries then
    return decisions.throw
  end

  return decisions.retry
end

function SimpleRetry:on_write_timeout(request_infos)
  if request_infos.n_retries > self.max_retries then
    return decisions.throw
  end

  return decisions.retry
end

return {
  decisions = decisions,
  simple_retry = SimpleRetry
}
