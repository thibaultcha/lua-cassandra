local math_fmod = math.fmod

local RoundRobin = {
  new = function(self)
    self.index = 0
  end,
  iterator = function(self)
    -- return an iterator to be used
    return function(hosts)
      local keys = {}
      for k in pairs(hosts) do
        keys[#keys + 1] = k
      end

      local n = #keys
      local counter = 0
      local plan_index = math_fmod(self.index, n)
      self.index = self.index + 1

      return function(t, i)
        local mod = math_fmod(plan_index, n) + 1

        plan_index = plan_index + 1
        counter = counter + 1

        if counter <= n then
          return mod, hosts[keys[mod]]
        end
      end
    end
  end
}

return {
  RoundRobin = function()
    RoundRobin:new()
    return RoundRobin
  end
}
