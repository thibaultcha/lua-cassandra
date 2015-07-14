local Object = require "cassandra.classic"

local _M = Object:extend()

function _M:new(marshaller, constants)
  self.marshaller = marshaller
  self.constants = constants
end

function _M:build_frame(op_code, body, tracing)
  local version = string.char(self.constants.version_codes.REQUEST)
  local flags = tracing and self.constants.flags.TRACING or "\000"
  local stream_id = "\000"
  local length = self.marshaller:int_representation(#body)
  local frame = version..flags..stream_id..string.char(op_code)..length..body
  return frame
end

-- Query: <query><query_parameters>
-- Batch: <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]
function _M:build_body(operation, args, options)
  local op_code, op_repr, op_parameters = "", "", ""
  if type(operation) == "string" then
    -- Raw string query
    op_code = self.constants.op_codes.QUERY
    op_repr = self.marshaller:long_string_representation(operation)
    op_parameters = self.marshaller:query_representation(args, options)
  elseif operation.id then
    -- Prepared statement
    op_code = self.constants.op_codes.EXECUTE
    op_repr = self.marshaller:short_bytes_representation(operation.id)
    op_parameters = self.marshaller:query_representation(args, options)
  elseif operation.is_batch_statement then
    -- Batch statement
    op_code = self.constants.op_codes.BATCH
    op_repr = self.marshaller:batch_representation(operation, options)
  end

  -- frame body
  return op_repr..op_parameters, op_code
end

return _M
