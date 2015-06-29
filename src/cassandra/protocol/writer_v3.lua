local Writer_v2 = require "cassandra.protocol.writer_v2"

local _M = Writer_v2:extend()

function _M:build_frame(op_code, body, tracing)
  local version = string.char(self.constants.version_codes.REQUEST)
  local flags = tracing and self.constants.flags.TRACING or "\000"
  local stream_id = self.marshaller.short_representation(0)
  local length = self.marshaller.int_representation(#body)
  local frame = version..flags..stream_id..string.char(op_code)..length..body
  return frame
end

return _M
