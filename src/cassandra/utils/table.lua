local _M = {}

function _M.extend_table(defaults, values)
  for k in pairs(defaults) do
    if values[k] == nil then
      values[k] = defaults[k]
    end
  end
end

function _M.is_array(t)
  local i = 0
  for _ in pairs(t) do
    i = i + 1
    if t[i] == nil and t[tostring(i)] == nil then return false end
  end
  return true
end

return _M
