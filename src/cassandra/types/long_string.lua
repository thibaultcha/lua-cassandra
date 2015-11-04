return {
  repr = function(self, str)
    return self:repr_int(#str)..str
  end,
  read = function(buffer)
    local n_bytes = buffer:read_int()
    return buffer:read(n_bytes)
  end
}
