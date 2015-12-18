local Object = require "cassandra.utils.classic"
local string_sub = string.sub

local Buffer = Object:extend()

function Buffer:new(version, str)
  self.version = version -- protocol version for properly encoding types
  self.str = str and str or ""
  self.pos = nil
  self.len = #self.str
  self:reset()
end

function Buffer:dump()
  return self.str
end

function Buffer:write(bytes)
  self.str = self.str..bytes
  self.len = self.len + #bytes
  self.pos = self.len
end

function Buffer:read(n_bytes_to_read)
  if n_bytes_to_read < 1 then return "" end
  local last_index = n_bytes_to_read ~= nil and self.pos + n_bytes_to_read - 1 or -1
  local bytes = string_sub(self.str, self.pos, last_index)
  self.pos = self.pos + #bytes
  return bytes
end

function Buffer:reset()
  self.pos = 1 -- lua indexes start at 1, remember?
end

return Buffer
