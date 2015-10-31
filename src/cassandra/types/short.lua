local utils = require "cassandra.utils"

return {
  write = function(self, val)
    self:write_bytes(utils.big_endian_representation(val, 2))
  end,
  read = function(self)
    local bytes = self:read_bytes(2)
    return utils.string_to_number(bytes, true)
  end
}
