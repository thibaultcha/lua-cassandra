local tostring = tostring
local pairs = pairs
local type = type

local _M = {}

function _M.is_array(t)
  if type(t) ~= "table" then
    return false
  end
  local i = 0
  for _ in pairs(t) do
    i = i + 1
    if t[i] == nil and t[tostring(i)] == nil then
      return false
    end
  end
  return true
end

return _M
