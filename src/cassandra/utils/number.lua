local concat = table.concat
local char = string.char
local byte = string.byte
local fmod = math.fmod
local rep = string.rep
local pow = math.pow

local insert = table.insert

local _M = {}

function _M.big_endian_representation(num, bytes)
  if num < 0 then
    -- 2's complement
    num = pow(0x100, bytes) + num
  end
  local t = {}
  while num > 0 do
    local rest = fmod(num, 0x100)
    insert(t, 1, char(rest))
    num = (num-rest) / 0x100
  end
  local padding = rep("\0", bytes - #t)
  return padding..concat(t)
end

function _M.string_to_number(str, signed)
  local number = 0
  local exponent = 1
  for i = #str, 1, -1 do
    number = number + byte(str, i) * exponent
    exponent = exponent * 256
  end
  if signed and number > exponent / 2 then
    -- 2's complement
    number = number - exponent
  end
  return number
end

return _M
