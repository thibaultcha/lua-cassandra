local string_gmatch = string.gmatch
local string_rep = string.rep
local string_sub = string.sub
local string_byte = string.byte
local string_format = string.format
local tonumber = tonumber
local table_insert = table.insert
local table_concat = table.concat

return {
  repr = function(self, value)
    local t = {}
    local hexadectets = {}
    local ip = value:lower():gsub("::",":0000:")

    if value:match(":") then
      -- ipv6
      for hdt in string_gmatch(ip, "[%x]+") do
        -- fill up hexadectets with 0 so all are 4 digits long
        hexadectets[#hexadectets + 1] = string_rep("0", 4 - #hdt)..hdt
      end
      for i, hdt in ipairs(hexadectets) do
        while hdt == "0000" and #hexadectets < 8 do
          table_insert(hexadectets, i + 1, "0000")
        end
        for j = 1, 4, 2 do
          table_insert(t, self:repr_byte(tonumber(string_sub(hdt, j, j + 1), 16)))
        end
      end
    else
      -- ipv4
      for d in string_gmatch(value, "(%d+)") do
        table_insert(t, self:repr_byte(d))
      end
    end

    return table_concat(t)
  end,
  read = function(buffer)
    local bytes = buffer:dump()
    buffer = {}
    if #bytes == 16 then
      -- ipv6
      for i = 1, #bytes, 2 do
        buffer[#buffer + 1] = string_format("%02x", string_byte(bytes, i))..string_format("%02x", string_byte(bytes, i + 1))
      end
      return table_concat(buffer, ":")
    else
      -- ipv4
      for i = 1, #bytes do
        buffer[#buffer + 1] = string_format("%d", string_byte(bytes, i))
      end
    end

    return table_concat(buffer, ".")
  end
}
