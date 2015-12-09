local log = require "cassandra.log"
local cache = require "cassandra.cache"
local math_pow = math.pow
local math_min = math.min

local function constant_reconnection_policy(delay)
  return {
    new_schedule = function() end,
    next = function()
      return delay
    end
  }
end

local function shared_exponential_reconnection_policy(base_delay, max_delay)
  return {
    new_schedule = function(host)
      local shm = host.options.shm
      local index_key = "exp_reconnection_idx_"..host.address
      local dict = cache.get_dict(shm)

      local ok, err = dict:set(index_key, 0)
      if not ok then
        log.err("Cannot reset schedule for shared exponential reconnection policy in shared dict "..shm..": "..err)
      end
    end,
    next = function(host)
      local shm = host.options.shm
      local index_key = "exp_reconnection_idx_"..host.address
      local dict = cache.get_dict(host.options.shm)

      local ok, err = dict:add(index_key, 0)
      if not ok and err ~= "exists" then
        log.err("Cannot prepare shared exponential reconnection policy in shared dict "..shm..": "..err)
      end

      local index = dict:incr(index_key, 1)

      local delay
      if index == nil or index > 64 then
        delay = max_delay
      else
        delay = math_min(math_pow(index, 2) * base_delay, max_delay)
      end

      return delay
    end
  }
end

return {
  Constant = constant_reconnection_policy,
  SharedExponential = shared_exponential_reconnection_policy
}
