return {
  write = function(self, str)
    self:write_short(#str)
    self:write_bytes(str)
  end,
  read = function(self)
    local n_bytes = self:read_short()
    return self:read_bytes(n_bytes)
  end
}
