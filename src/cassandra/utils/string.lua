local string_format = string.format
local string_gsub = string.gsub

local _M = {}

function _M.split_by_colon(str)
  local fields = {}
  str:gsub("([^:]+)", function(c) fields[#fields+1] = c end)
  return fields[1], fields[2]
end

function _M.split(str, sep)
  local sep, fields = sep or ":", {}
  local pattern = string_format("([^%s]+)", sep)
  string_gsub(str, pattern, function(c) fields[#fields+1] = c end)
  return fields
end

return _M
