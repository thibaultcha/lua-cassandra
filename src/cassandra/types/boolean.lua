return {
  write = function(self, val)
    if val then
      self:write_byte(1)
    else
      self:write_byte(0)
    end
  end,
  read = function(self)
    return self:read_byte() == 1
  end
}
