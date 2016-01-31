-- Logging wrapper
-- lua-cassandra is built with support for pure Lua, outside of ngx_lua,
-- this module provides a fallback to `print` when lua-cassandra runs
-- outside of ngx_lua.

local is_ngx = ngx ~= nil
local ngx_log = is_ngx and ngx.log
local ngx_get_phase = is_ngx and ngx.get_phase

local LEVELS = {
  ERR = 1,
  WARN = 2,
  INFO = 3,
  DEBUG = 4
}

local log = {}

for lvl_name, lvl in pairs(LEVELS) do
  log[lvl_name:lower()] = function(...)
    if is_ngx and ngx_get_phase() ~= "init" then
      ngx_log(ngx[lvl_name], ...)
    end
  end
end

return log
