local string_char = string.char
local string_byte = string.byte

return {
  repr = function(self, val)
    return string_char(val)
  end,
  read = function(self)
    return string_byte(self:read(1))
  end
}
