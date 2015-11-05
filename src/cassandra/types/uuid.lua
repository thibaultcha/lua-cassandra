local string_gsub = string.gsub
local string_sub = string.sub
local string_format = string.format
local table_insert = table.insert
local table_concat = table.concat

return {
  repr = function(self, val)
    local repr = {}
    local str = string_gsub(val, "-", "")
    for i = 1, #str, 2 do
      local byte_str = string_sub(str, i, i + 1)
      table_insert(repr, self:repr_byte(tonumber(byte_str, 16)))
    end
    return table_concat(repr)
  end,
  read = function(buffer)
    local uuid = {}
    for i = 1, buffer.len do
      uuid[i] = string_format("%02x", buffer:read_byte())
    end
    table_insert(uuid, 5, "-")
    table_insert(uuid, 8, "-")
    table_insert(uuid, 11, "-")
    table_insert(uuid, 14, "-")
    return table_concat(uuid)
  end
}
