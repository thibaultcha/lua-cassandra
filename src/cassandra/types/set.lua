return {
  repr = function(self, set)
    local n
    if self.version < 3 then
      n = self:repr_short(#set)
    else
      n = self:repr_int(#set)
    end
    for _, val in ipairs(set) do
      -- @TODO write_value infering the type
    end
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
