local string_format = string.format

local LEVELS = {
  "ERR",
  "INFO",
  "DEBUG"
}

local _LOG = {}

local function log(level, message)
  if ngx and type(ngx.log) == "function" then
    -- lua-nginx-module
    ngx.log(ngx[level], message)
  else
    print(string_format("%s: %s", level, message)) -- can't configure level for now
  end
end

for _, level in ipairs(LEVELS) do
  _LOG[level:lower()] = function(message)
    log(level, message)
  end
end

return _LOG
