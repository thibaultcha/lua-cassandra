return {
  write = function(self, map)
    local n = #map
    for k, v in pairs(map) do
      n = n + 1
    end
    self:write_short(n)
    for k, v in pairs(map) do
      self:write_string(k)
      self:write_string(v)
    end
  end,
  read = function(self)
    local map = {}
    local n_strings = self:read_short()
    for _ = 1, n_strings do
      local key = self:read_string()
      local value = self:read_string()
      map[key] = value
    end
    return map
  end
}
