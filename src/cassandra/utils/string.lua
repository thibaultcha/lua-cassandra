local _M = {}

function _M.split_by_colon(str)
  local fields = {}
  str:gsub("([^:]+)", function(c) fields[#fields+1] = c end)
  return fields[1], fields[2]
end

return _M
