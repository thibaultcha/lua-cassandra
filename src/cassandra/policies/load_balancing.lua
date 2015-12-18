local log = require "cassandra.log"
local cache = require "cassandra.cache"
local index_key = "rr_index"
local math_fmod = math.fmod

return {
  SharedRoundRobin = function(shm, hosts)
    local n = #hosts
    local counter = 0

    local dict = cache.get_dict(shm)

    local ok, err = dict:add(index_key, -1)
    if not ok and err ~= "exists" then
      log.err("Cannot prepare shared round robin load balancing policy in shared dict "..shm..": "..err)
    end

    local index, err = dict:incr(index_key, 1)
    if err then
      log.err("Cannot increment shared round robin load balancing policy index in shared dict "..shm..": "..err)
    elseif index == nil then
      index = 0
    end

    local plan_index = math_fmod(index or 0, n)

    return function(t, i)
      local mod = math_fmod(plan_index, n) + 1
      plan_index = plan_index + 1
      counter = counter + 1

      if counter <= n then
        return mod, hosts[mod]
      end
    end
  end
}
