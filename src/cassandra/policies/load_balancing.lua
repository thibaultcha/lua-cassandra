local log = require "cassandra.log"
local cache = require "cassandra.cache"
local math_fmod = math.fmod

return {
  SharedRoundRobin = function(shm, hosts)
    local n = #hosts
    local counter = 0

    local dict = cache.get_dict(shm)
    local ok, err = dict:add("rr_plan_index", 0)
    if not ok and err ~= "exists" then
      log.err("Cannot prepare shared round robin load balancing policy: "..err)
    end

    return function(t, i)
      local plan_index = dict:get("rr_plan_index")
      local mod = math_fmod(plan_index, n) + 1
      dict:incr("rr_plan_index", 1)
      counter = counter + 1

      if counter <= n then
        return mod, hosts[mod]
      end
    end
  end
}
