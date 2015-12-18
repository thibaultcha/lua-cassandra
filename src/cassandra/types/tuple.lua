local table_concat = table.concat

return {
  repr = function(self, values)
    local repr = {}
    for _, v in ipairs(values) do
      repr[#repr + 1] = self:repr_cql_value(v)
    end
    return table_concat(repr)
  end,
  read = function(buffer, type)
    local tuple = {}
    for _, field in ipairs(type.fields) do
      tuple[#tuple + 1] = buffer:read_cql_value(field.type)
    end
    return tuple
  end
}
