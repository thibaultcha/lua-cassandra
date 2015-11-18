local types = require "cassandra.types"


return {
  read = function(buffer)
    local type_id = buffer:read_short()
    local type_value
    if type_id == types.cql_types.set then
      type_value = buffer:read_options()
    elseif type_id == types.cql_types.map then
      type_value = {buffer:read_options(), buffer:read_options()}
    end

    -- @TODO support non-native types (custom, map, list, set, UDT, tuple)
    return {type_id = type_id, value_type_id = type_value}
  end
}
