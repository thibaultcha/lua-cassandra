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

-- lua-cjson (by Mark Pulford)
-- https://github.com/mpx/lua-cjson/blob/master/lua/cjson/util.lua
-- Modified to not allow any sparse array.
--
-- Determine with a Lua table can be treated as an array.
-- Explicitly returns "not an array" for very sparse arrays.
-- Returns:
-- -1   Not an array
-- 0    Empty table
-- >0   Highest index in the array
function _M.is_array(table)
  local max = 0
  local count = 0
  for k, v in pairs(table) do
    if type(k) == "number" then
      if k > max then max = k end
      count = count + 1
    else
      return -1
    end
  end
  if max > count then
    return -1
  end

  return max
end

function _M.trim(s)
  return (s:gsub("^%s*(.-)%s*$", "%1"))
end

return _M
