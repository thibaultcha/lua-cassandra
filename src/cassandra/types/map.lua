local table_insert = table.insert
local table_concat = table.concat

return {
  repr = function(self, map)
    local repr = {}
    local size = 0

    for key, value in pairs(map) do
      repr[#repr + 1] = self:repr_cql_value(key)
      repr[#repr + 1] = self:repr_cql_value(value)
      size = size + 1
    end

    if self.version < 3 then
      table_insert(repr, 1, self:repr_short(size))
    else
      table_insert(repr, 1, self:repr_int(size))
    end

    return table_concat(repr)
  end,
  read = function(buffer, type)
    local map = {}
    local key_type = type[1]
    local value_type = type[2]

    local n
    if buffer.version < 3 then
      n = buffer:read_short()
    else
      n = buffer:read_int()
    end

    for _ = 1, n do
      local key = buffer:read_cql_value(key_type)
      map[key] = buffer:read_cql_value(value_type)
    end

    return map
  end
}
