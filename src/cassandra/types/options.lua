local CQL_TYPES = require "cassandra.types.cql_types"

return {
  read = function(buffer)
    local type_id = buffer:read_short()
    local type_value
    if type_id == CQL_TYPES.set then
      type_value = buffer:read_options()
    elseif type_id == CQL_TYPES.map then
      type_value = {buffer:read_options(), buffer:read_options()}
    end

    -- @TODO support non-native types (custom, map, list, set, UDT, tuple)
    return {id = type_id, value = type_value}
  end
}
