local int = require "cassandra.types.int"

return {
  repr = function(self, val)
    return int.repr(nil, #val)..val
  end,
  read = function(self)
    local n_bytes = int.read(self)
    return self:read(n_bytes)
  end
}