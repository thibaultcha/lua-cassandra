local Buffer = require "cassandra.utils.buffer"

local TYPES = {
  "byte",
  "short",
  "boolean",
  "integer",
  "string",
  "string_map"
}

for _, buf_type in ipairs(TYPES) do
  local mod = require("cassandra.types."..buf_type)
  Buffer["read_"..buf_type] = mod.read
  Buffer["write_"..buf_type] = mod.write
end

return Buffer
