local utils = require "cassandra.utils"

local _M = {}

function _M.build_frame(session, op_code, body, tracing)
  local version = string.char(session.constants.version_codes.REQUEST)
  local flags = tracing and "\002" or "\000"
  local stream_id = "\000"
  local length = session.marshaller.int_representation(#body)
  local frame = version..flags..stream_id..string.char(op_code)..length..body
  return frame
end

function _M.build_body(session, operation, args, options)
  local op_code, op_repr
  if type(operation) == "string" then
    -- Raw string query
    op_code = session.constants.op_codes.QUERY
    op_repr = session.marshaller.long_string_representation(operation)
  elseif operation.id then
    -- Prepared statement
    op_code = session.constants.op_codes.EXECUTE
    op_repr = session.marshaller.short_bytes_representation(operation.id)
  end

  -- Flags of the <query_parameters>
  local flags_repr = 0
  if args then
    flags_repr = utils.setbit(flags_repr, session.constants.query_flags.VALUES)
  end

  -- TODO: implement options
  local result_page_size = ""
  local paging_state = ""

  -- <query_parameters>: <consistency><flags>[<n><value_i><...>][<result_page_size>][<paging_state>]
  local query_parameters = session.marshaller.short_representation(options.consistency_level)..string.char(flags_repr)..session.marshaller.values_representation(args)..result_page_size..paging_state

  -- frame body: <query><query_parameters>
  return op_repr..query_parameters, op_code
end

return _M
