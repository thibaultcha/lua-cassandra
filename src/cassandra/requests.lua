local CONSTS = require "cassandra.consts"
local Buffer = require "cassandra.buffer"
local frame_header = require "cassandra.types.frame_header"

local op_codes = frame_header.op_codes
local FrameHeader = frame_header.FrameHeader

--- Request
-- @section request

local Request = Buffer:extend()

function Request:new(options)
  if options == nil then options = {} end

  self.version = options.version and options.version or CONSTS.DEFAULT_PROTOCOL_VERSION
  self.op_code = options.op_code

  Request.super.new(self, nil, self.version)
end

function Request:write(flags)
  if not self.op_code then error("Request#write() has no op_code") end

  local frameHeader = FrameHeader(self.version, flags, self.op_code, self.len)
  return frameHeader:write()..Request.super.write(self)
end

--- StartupRequest
-- @section startup_request

local StartupRequest = Request:extend()

function StartupRequest:new(...)
  StartupRequest.super.new(self, ...)

  self.op_code = op_codes.STARTUP
  StartupRequest.super.write_string_map(self, {
    CQL_VERSION = CONSTS.CQL_VERSION
  })
end

return {
  Request = Request,
  StartupRequest = StartupRequest
}
