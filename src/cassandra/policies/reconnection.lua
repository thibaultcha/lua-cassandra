local pow = math.pow
local min = math.min

local function constant(delay)
  return {
    name = "constant",
    reset = function() return true end,
    get_next = function() return delay end
  }
end

local key = "shared_exp_idx_"
local function shared_exp(shm, base_delay, max_delay)
  return {
    name = "shared_exp",
    reset = function(host)
      local index_key = key..host

      local ok, err = shm:set(index_key, 0)
      if not ok then return nil, err end

      return true
    end,
    get_next = function(host)
      local index_key = key..host

      local ok, err = shm:add(index_key, 0)
      if not ok and err ~= "exists" then

      end

      local index = shm:incr(index_key, 1)

      return index == nil or index > 64 and
        max_delay
        or
        min(pow(index, 2) * base_delay, max_delay)
    end
  }
end

return {
  constant = constant,
  shared_exp = shared_exp
}
