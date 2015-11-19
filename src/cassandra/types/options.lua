local types = require "cassandra.types"

return {
  read = function(buffer)
    local type_id = buffer:read_short()
    local type_value
    if type_id == types.cql_types.set or type_id == types.cql_types.list then
      type_value = buffer:read_options()
    elseif type_id == types.cql_types.map then
      type_value = {buffer:read_options(), buffer:read_options()}
    elseif type_id == types.cql_types.udt then
      type_value = buffer:read_udt_type()
    elseif type_id == types.cql_types.tuple then
      type_value = buffer:read_tuple_type()
    end

    return {type_id = type_id, value_type_id = type_value}
  end
}
