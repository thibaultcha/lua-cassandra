local _M = {}

function _M.big_endian_representation(num, bytes)
  if num < 0 then
    -- 2's complement
    num = math.pow(0x100, bytes) + num
  end
  local t = {}
  while num > 0 do
    local rest = math.fmod(num, 0x100)
    table.insert(t, 1, string.char(rest))
    num = (num-rest) / 0x100
  end
  local padding = string.rep(string.char(0), bytes - #t)
  return padding .. table.concat(t)
end

function _M.string_to_number(str, signed)
  local number = 0
  local exponent = 1
  for i = #str, 1, -1 do
    number = number + string.byte(str, i) * exponent
    exponent = exponent * 256
  end
  if signed and number > exponent / 2 then
    -- 2's complement
    number = number - exponent
  end
  return number
end

return _M
