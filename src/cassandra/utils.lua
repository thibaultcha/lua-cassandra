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

return _M
