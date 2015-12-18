return {
  repr = function(self, str)
    return self:repr_short(#str)..str
  end,
  read = function(buffer)
    local n_bytes = buffer:read_short()
    return buffer:read(n_bytes)
  end
}
