local string_format = string.format
local unpack = unpack
local type = type

local LEVELS = {
  ["ERR"] = 1,
  ["WARN"] = 2,
  ["INFO"] = 3,
  ["DEBUG"] = 4
}

local function default_print_handler(self, level, level_name, ...)
  local arg = {...}
  if level <= self.print_lvl then
    print(string_format("%s -- %s", level_name, unpack(arg)))
  end
end

local Log = {}
Log.__index = Log

for log_level_name, log_level in pairs(LEVELS) do
  Log[log_level_name:lower()] = function(self, ...)
    if ngx and type(ngx.log) == "function" then
      -- lua-nginx-module
      ngx.log(ngx[log_level_name], ...)
    else
      self:print_handler(log_level, log_level_name:lower(), ...)
    end
  end
end

function Log:__call(print_lvl, print_handler)
  return setmetatable({
    print_lvl = print_lvl and LEVELS[print_lvl] or LEVELS.ERR,
    print_handler = print_handler and print_handler or default_print_handler
  }, Log)
end

return setmetatable({}, Log)
