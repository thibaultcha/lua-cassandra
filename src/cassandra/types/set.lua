local table_concat = table.concat
local table_insert = table.insert

return {
  repr = function(self, set)
    local repr = {}
    if self.version < 3 then
      table_insert(repr, self:repr_short(#set))
    else
      table_insert(repr, self:repr_int(#set))
    end
    for _, val in ipairs(set) do
      table_insert(repr, self:repr_cql_value(val))
    end

    return table_concat(repr)
  end,
  read = function(buffer, value_type)
    local n
    local set = {}
    if buffer.version < 3 then
      n = buffer:read_short()
    else
      n = buffer:read_int()
    end
    for _ = 1, n do
      set[#set + 1] = buffer:read_cql_value(value_type)
    end

    return set
  end
}
