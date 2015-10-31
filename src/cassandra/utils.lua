local CONSTS = require "cassandra.consts"

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

math.randomseed(os.time())

-- @see http://en.wikipedia.org/wiki/Fisher-Yates_shuffle
function _M.shuffle_array(arr)
  local n = #arr
  while n >= 2 do
    local k = math.random(n)
    arr[n], arr[k] = arr[k], arr[n]
    n = n - 1
  end
  return arr
end

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

function _M.split_by_colon(str)
  local fields = {}
  str:gsub("([^:]+)", function(c) fields[#fields+1] = c end)
  return fields[1], fields[2]
end

function _M.hasbit(x, p)
  return x % (p + p) >= p
end

function _M.setbit(x, p)
  return _M.hasbit(x, p) and x or x + p
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

local rawget = rawget

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
