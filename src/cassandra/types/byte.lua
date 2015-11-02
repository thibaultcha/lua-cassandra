local string_char = string.char
local string_byte = string.byte

return {
  write = function(self, val)
    self:write_bytes(string_char(val))
  end,
  read = function(self)
    local byte = self:read_bytes(1)
    return string_byte(byte)
  end
}
