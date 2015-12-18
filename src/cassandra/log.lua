-- Logging wrapper
-- lua-cassandra is built with support for pure Lua, outside of ngx_lua,
-- this module provides a fallback to `print` when lua-cassandra runs
-- outside of ngx_lua.

local is_ngx = ngx ~= nil
local ngx_log = is_ngx and ngx.log
local ngx_get_phase = is_ngx and ngx.get_phase
local string_format = string.format
local print = print

-- ngx_lua levels redefinition when outside of ngx_lua.
local LEVELS = {
  QUIET = 0,
  ERR = 1,
  WARN = 2,
  INFO = 3,
  DEBUG = 4
}

-- Default logging level when outside of ngx_lua.
local cur_lvl = LEVELS.INFO
local cur_fmt = "%s -- %s"

local log = {}

function log.set_lvl(lvl_name)
  if LEVELS[lvl_name] ~= nil then
    cur_lvl = LEVELS[lvl_name]
  end
end

function log.get_lvl()
  return cur_lvl
end

function log.set_format(fmt)
  cur_fmt = fmt
end

-- Makes this module testable by spying on this function
function log.print(str)
  print(str)
end

for lvl_name, lvl in pairs(LEVELS) do
  log[lvl_name:lower()] = function(...)
    if is_ngx and ngx_get_phase() ~= "init" then
      ngx_log(ngx[lvl_name], ...)
    elseif lvl <= cur_lvl then
      log.print(string_format(cur_fmt, lvl_name, ...))
    end
  end
end

return log
