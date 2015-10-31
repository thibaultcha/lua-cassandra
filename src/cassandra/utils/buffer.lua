local Object = require "cassandra.classic"
local string_sub = string.sub
local table_insert = table.insert
local table_concat = table.concat

local Buffer = Object:extend()

function Buffer:new(str, version)
  self.version = version -- protocol version
  self.str = str and str or ""
  self.pos = 1 -- lua indexes start at 1, remember?
  self.len = #self.str
end

function Buffer:write()
  return self.str
end

function Buffer:write_bytes(value)
  self.str = self.str..value
  self.len = self.len + #value
  self.pos = self.len
end

function Buffer:read_bytes(n_bytes_to_read)
  local last_index = n_bytes_to_read ~= nil and self.pos + n_bytes_to_read - 1 or -1
  local bytes = string_sub(self.str, self.pos, last_index)
  self.pos = self.pos + #bytes  return bytes
end

return Buffer
