local string_char = string.char
local string_byte = string.byte
local math_ldexp = math.ldexp
local math_frexp = math.frexp
local math_floor = math.floor

return {
  repr = function(self, number)
    local sign = 0
    if number < 0.0 then
      sign = 0x80
      number = -number
    end
    local mantissa, exponent = math_frexp(number)
    if mantissa ~= mantissa then
      return string_char(0xFF, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- nan
    elseif mantissa == math.huge then
      if sign == 0 then
        return string_char(0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- +inf
      else
        return string_char(0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- -inf
      end
    elseif mantissa == 0.0 and exponent == 0 then
      return string_char(sign, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- zero
    else
      exponent = exponent + 0x3FE
      mantissa = (mantissa * 2.0 - 1.0) * math_ldexp(0.5, 53)
      return string_char(sign + math_floor(exponent / 0x10),
                         (exponent % 0x10) * 0x10 + math_floor(mantissa / 0x1000000000000),
                         math_floor(mantissa / 0x10000000000) % 0x100,
                         math_floor(mantissa / 0x100000000) % 0x100,
                         math_floor(mantissa / 0x1000000) % 0x100,
                         math_floor(mantissa / 0x10000) % 0x100,
                         math_floor(mantissa / 0x100) % 0x100,
                         mantissa % 0x100)
    end
  end,
  read = function(buffer)
    local bytes = buffer:read(8)
    local b1, b2, b3, b4, b5, b6, b7, b8 = string_byte(bytes, 1, 8)
    local sign = b1 > 0x7F
    local exponent = (b1 % 0x80) * 0x10 + math_floor(b2 / 0x10)
    local mantissa = ((((((b2 % 0x10) * 0x100 + b3) * 0x100 + b4) * 0x100 + b5) * 0x100 + b6) * 0x100 + b7) * 0x100 + b8
    if sign then
      sign = -1
    else
      sign = 1
    end
    local number
    if mantissa == 0 and exponent == 0 then
      number = sign * 0.0
    elseif exponent == 0x7FF then
      if mantissa == 0 then
        number = sign * math.huge
      else
        number = 0.0/0.0
      end
    else
      number = sign * math_ldexp(1.0 + mantissa / 0x10000000000000, exponent - 0x3FF)
    end
    return number
  end
}
