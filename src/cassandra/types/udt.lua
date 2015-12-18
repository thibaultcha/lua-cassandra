local table_concat = table.concat

return {
  -- values must be ordered as they are defined in the UDT declaration
  repr = function(self, values)
    local repr = {}
    for _, v in ipairs(values) do
      repr[#repr + 1] = self:repr_cql_value(v)
    end
    return table_concat(repr)
  end,
  read = function(buffer, type)
    local udt = {}
    for _, field in ipairs(type.fields) do
      udt[field.name] = buffer:read_cql_value(field.type)
    end
    return udt
  end
}
