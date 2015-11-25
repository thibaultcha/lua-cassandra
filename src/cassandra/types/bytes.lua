local int = require "cassandra.types.int"
local type = type

return {
  repr = function(self, val)
    if type(val) == "table" and val.unset then
      return int.repr(nil, -2)
    end

    return int.repr(nil, #val)..val
  end,
  read = function(self)
    local n_bytes = self:read_int()
    if n_bytes < 0 then
      return nil
    end
    return self:read(n_bytes)
  end
}
