--- Logging wrapper
-- lua-cassandra is built with support for pure Lua, outside of ngx_lua,
-- this module provides a fallback to `print` when lua-cassandra runs
-- outside of ngx_lua.

local is_ngx = ngx ~= nil
local ngx_log = is_ngx and ngx.log
local string_format = string.format

-- ngx_lua levels redefinition for helpers and
-- when outside of ngx_lua.
local LEVELS = {
  ERR = 1,
  WARN = 2,
  INFO = 3,
  DEBUG = 4
}

-- Default logging level when outside of ngx_lua.
local cur_lvl = LEVELS.INFO

local log = {}

function log.set_lvl(lvl_name)
  if is_ngx then return end
  if LEVELS[lvl_name] ~= nil then
    cur_lvl = LEVELS[lvl_name]
  end
end

for lvl_name, lvl in pairs(LEVELS) do
  log[lvl_name:lower()] = function(...)
    if is_ngx and ngx.get_phase() ~= "init" then
      ngx_log(ngx[lvl_name], ...)
    elseif lvl <= cur_lvl then
      print(string_format("%s -- %s", lvl_name, ...))
    end
  end
end

return log
