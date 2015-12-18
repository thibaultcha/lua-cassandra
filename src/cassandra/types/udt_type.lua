return {
  read = function(buffer)
    local udt_ks_name = buffer:read_string()
    local udt_name = buffer:read_string()

    local n = buffer:read_short()
    local fields = {}
    for _ = 1, n do
      fields[#fields + 1] = {
        name = buffer:read_string(),
        type = buffer:read_options()
      }
    end
    return {
      udt_name = udt_name,
      udt_keyspace = udt_ks_name,
      fields = fields
    }
  end
}
