return {
  repr = function(self, bytes)
    return bytes
  end,
  read = function(buffer)
    return buffer:dump()
  end
}
