local utils = require "cassandra.utils.number"

return {
  repr = function(self, val)
    return utils.big_endian_representation(val, 4)
  end,
  read = function(buffer)
    local bytes = buffer:read(4)
    return utils.string_to_number(bytes, true)
  end
}
