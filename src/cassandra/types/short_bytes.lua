local short = require "cassandra.types.short"

return {
  repr = function(self, val)
    return short.repr(nil, #val)..val
  end,
  read = function(self)
    local n_bytes = self:read_short()
    return self:read(n_bytes)
  end
}
