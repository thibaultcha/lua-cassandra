local utils = require "cassandra.utils"
local writer_v2 = require "cassandra.protocol.writer_v2"

local _M = utils.deep_copy(writer_v2)

function _M.build_frame(session, op_code, body, tracing)
  local version = string.char(session.constants.version_codes.REQUEST)
  local flags = tracing and session.constants.flags.TRACING or "\000"
  local stream_id = session.marshaller.short_representation(0)
  local length = session.marshaller.int_representation(#body)
  local frame = version..flags..stream_id..string.char(op_code)..length..body
  return frame
end

return _M
