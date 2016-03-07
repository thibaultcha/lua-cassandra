local _index_key = "shm_rr_index"
local fmod = math.fmod

return {
  shm_round_robin = function(shm, hosts)
    local n = #hosts
    local counter = 0

    local ok, err = shm:add(_index_key, -1)
    if not ok and err ~= "exists" then
      return nil, "could not prepare shm round robin load balancing policy: "..err
    end

    local index, err = shm:incr(_index_key, 1)
    if err then
      return nil, "could not increment shm round robin load balancing policy index: "..err
    elseif index == nil then
      index = 0
    end

    local plan_index = fmod(index or 0, n)

    return function(t, i)
      local mod = fmod(plan_index, n) + 1
      plan_index = plan_index + 1
      counter = counter + 1

      if counter <= n then
        return mod, hosts[mod]
      end
    end
  end
}
