local string_char = string.char
local math_floor = math.floor
local string_byte = string.byte

return {
  repr = function(self, val)
    local first_byte = val >= 0 and 0 or 0xFF
    return string_char(first_byte, -- only 53 bits from double
                       math_floor(val / 0x1000000000000) % 0x100,
                       math_floor(val / 0x10000000000) % 0x100,
                       math_floor(val / 0x100000000) % 0x100,
                       math_floor(val / 0x1000000) % 0x100,
                       math_floor(val / 0x10000) % 0x100,
                       math_floor(val / 0x100) % 0x100,
                       val % 0x100)
  end,
  read = function(buffer)
    local bytes = buffer:read(8)
    local b1, b2, b3, b4, b5, b6, b7, b8 = string_byte(bytes, 1, 8)
    if b1 < 0x80 then
      return ((((((b1 * 0x100 + b2) * 0x100 + b3) * 0x100 + b4) * 0x100 + b5) * 0x100 + b6) * 0x100 + b7) * 0x100 + b8
    else
      return ((((((((b1 - 0xFF) * 0x100 + (b2 - 0xFF)) * 0x100 + (b3 - 0xFF)) * 0x100 + (b4 - 0xFF)) * 0x100 + (b5 - 0xFF)) * 0x100 + (b6 - 0xFF)) * 0x100 + (b7 - 0xFF)) * 0x100 + (b8 - 0xFF)) - 1
    end
  end
}
