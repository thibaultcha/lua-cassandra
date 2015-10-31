local utils = require "cassandra.utils"

return {
  read = function(self)
    local bytes = self:read_bytes(4)
    return utils.string_to_number(bytes, true)
  end,
  write = function(self, val)
    self:write_bytes(utils.big_endian_representation(val, 4))
  end
}
