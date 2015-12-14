local setmetatable = setmetatable
local getmetatable = getmetatable
local table_remove = table.remove
local tostring = tostring
local ipairs = ipairs
local pairs = pairs
local type = type

local _M = {}

function _M.extend_table(...)
  local sources = {...}
  local values = table_remove(sources)

  for _, source in ipairs(sources) do
    for k in pairs(source) do
      if values[k] == nil then
        values[k] = source[k]
      end
      if type(source[k]) == "table" and type(values[k]) == "table" then
        _M.extend_table(source[k], values[k])
      end
    end
  end

  return values
end

function _M.deep_copy(orig)
  local copy
  if type(orig) == "table" then
    copy = {}
    for orig_key, orig_value in next, orig, nil do
      copy[_M.deep_copy(orig_key)] = _M.deep_copy(orig_value)
    end
    setmetatable(copy, _M.deep_copy(getmetatable(orig)))
  else
    copy = orig
  end
  return copy
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
