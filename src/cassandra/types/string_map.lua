local table_concat = table.concat

return {
  repr = function(self, map)
    local t = {}
    local n = 0
    for k, v in pairs(map) do
      n = n + 1
    end
    t[1] = self:repr_short(n)
    for k, v in pairs(map) do
      t[#t + 1] = self:repr_string(k)
      t[#t + 1] = self:repr_string(v)
    end
    return table_concat(t)
  end,
  read = function(buffer)
    local map = {}
    local n_strings = buffer:read_short()
    for _ = 1, n_strings do
      local key = buffer:read_string()
      local value = buffer:read_string()
      map[key] = value
    end
    return map
  end
}
