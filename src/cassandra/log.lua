local string_format = string.format
local unpack = unpack
local type = type

local LEVELS = {
  "ERR",
  "INFO",
  "DEBUG"
}

local _LOG = {}

local function log(level, ...)
  local arg = {...}
  if ngx and type(ngx.log) == "function" then
    -- lua-nginx-module
    ngx.log(ngx[level], unpack(arg))
  else
    print(string_format("%s: ", level), unpack(arg)) -- can't configure level for now
  end
end

for _, level in ipairs(LEVELS) do
  _LOG[level:lower()] = function(...)
    log(level, ...)
  end
end

return _LOG
