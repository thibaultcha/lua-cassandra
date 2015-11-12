local cache = require "cassandra.cache"
local log = require "cassandra.log"
local math_fmod = math.fmod

return {
  RoundRobin = function(shm, hosts)
    local n = #hosts
    local counter = 0

    local dict = cache.get_dict(shm)
    local ok, err = dict:add("plan_index", 0)
    if not ok then
      log.err("Cannot prepare round robin load balancing policy: "..err)
    end

    return function(t, i)
      local plan_index = dict:get("plan_index")
      local mod = math_fmod(plan_index, n) + 1
      dict:incr("plan_index", 1)
      counter = counter + 1

      if counter <= n then
        return mod, hosts[mod]
      end
    end
  end
}
