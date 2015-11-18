local CONSTS = require "cassandra.constants"

local _M = {}

function _M.extend_table(defaults, values)
  if values == nil then values = {} end
  for k in pairs(defaults) do
    if values[k] == nil then
      values[k] = defaults[k]
    end
    if type(defaults[k]) == "table" then
      _M.extend_table(defaults[k], values[k])
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

local _const_mt = {
  get = function(t, key, version)
    if not version then version = CONSTS.MAX_PROTOCOL_VERSION end

    local const, version_consts
    while version >= CONSTS.MIN_PROTOCOL_VERSION and const == nil do
      version_consts = t[version] ~= nil and t[version] or t
      const = rawget(version_consts, key)
      version = version - 1
    end
    return const
  end
}

_const_mt.__index = _const_mt

_M.const_mt = _const_mt

return _M
