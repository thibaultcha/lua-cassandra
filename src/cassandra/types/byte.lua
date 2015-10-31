local utils = require "cassandra.utils"

return {
  write = function(self, val)
    self:write_bytes(utils.big_endian_representation(val, 1))
  end,
  read = function(self)
    local byte = self:read_bytes(1)
    return utils.string_to_number(byte, true)
  end
}
