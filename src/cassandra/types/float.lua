local string_char = string.char
local string_byte = string.byte
local math_ldexp = math.ldexp
local math_frexp = math.frexp
local math_floor = math.floor

return {
  repr = function(self, number)
    if number == 0 then
      return string_char(0x00, 0x00, 0x00, 0x00)
    elseif number ~= number then
      return string_char(0xFF, 0xFF, 0xFF, 0xFF)
    else
      local sign = 0x00
      if number < 0 then
        sign = 0x80
        number = -number
      end
      local mantissa, exponent = math_frexp(number)
      exponent = exponent + 0x7F
      if exponent <= 0 then
        mantissa = math_ldexp(mantissa, exponent - 1)
        exponent = 0
      elseif exponent > 0 then
        if exponent >= 0xFF then
          return string_char(sign + 0x7F, 0x80, 0x00, 0x00)
        elseif exponent == 1 then
          exponent = 0
        else
          mantissa = mantissa * 2 - 1
          exponent = exponent - 1
        end
      end
      mantissa = math_floor(math_ldexp(mantissa, 23) + 0.5)
      return string_char(sign + math_floor(exponent / 2),
                         (exponent % 2) * 0x80 + math_floor(mantissa / 0x10000),
                         math_floor(mantissa / 0x100) % 0x100,
                         mantissa % 0x100)
    end
  end,
  read = function(buffer)
    local bytes = buffer:read(4)
    local b1, b2, b3, b4 = string_byte(bytes, 1, 4)
    local exponent = (b1 % 0x80) * 0x02 + math_floor(b2 / 0x80)
    local mantissa = math_ldexp(((b2 % 0x80) * 0x100 + b3) * 0x100 + b4, -23)
    if exponent == 0xFF then
      if mantissa > 0 then
        return 0 / 0
      else
        mantissa = math.huge
        exponent = 0x7F
      end
    elseif exponent > 0 then
      mantissa = mantissa + 1
    else
      exponent = exponent + 1
    end
    if b1 >= 0x80 then
      mantissa = -mantissa
    end
    return math_ldexp(mantissa, exponent - 0x7F)
  end
}
