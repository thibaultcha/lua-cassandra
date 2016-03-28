local simple = {}
simple.__index = simple

function simple.new(max_retries)
  return setmetatable({max_retries = max_retries}, simple)
end

function simple:on_unavailable(request_infos)
  return false
end

function simple:on_read_timeout(request_infos)
  return request_infos.n_retries < self.max_retries
end

function simple:on_write_timeout(request_infos)
  return request_infos.n_retries < self.max_retries
end

return {
  simple = simple
}
