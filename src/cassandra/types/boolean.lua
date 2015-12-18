return {
  repr = function(self, val)
    if val then
      return self:repr_byte(1)
    else
      return self:repr_byte(0)
    end
  end,
  read = function(buffer)
    local byte = buffer:read_byte()
    return byte == 1
  end
}
