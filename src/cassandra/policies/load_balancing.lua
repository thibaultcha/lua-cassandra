local storage = require "cassandra.storage"
local math_fmod = math.fmod

return {
  RoundRobin = function(shm, hosts)
    local n = #hosts
    local counter = 0

    local dict = storage.get_dict(shm)
    local plan_index = dict:get("plan_index")
    if not plan_index then
      dict:set("plan_index", 0)
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
