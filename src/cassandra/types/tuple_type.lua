return {
  read = function(buffer, type)
    local n = buffer:read_short()
    local fields = {}
    for _ = 1, n do
      fields[#fields + 1] = {
        type = buffer:read_options()
      }
    end
    return {
      fields = fields
    }
  end
}
