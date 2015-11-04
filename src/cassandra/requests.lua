local CONSTS = require "cassandra.consts"
local Object = require "cassandra.classic"
local Buffer = require "cassandra.buffer"
local frame_header = require "cassandra.types.frame_header"

local op_codes = frame_header.op_codes
local FrameHeader = frame_header.FrameHeader

--- Request
-- @section request

local Request = Object:extend()

function Request:new(op_code)
  self.version = nil
  self.flags = 0
  self.op_code = op_code
  self.frameBody = Buffer() -- no version
  self.built = false

  Request.super.new(self)
end

function Request:set_version(version)
  self.version = version
  self.frameBody.version = version
end

function Request:build()
  error("mest be implemented")
end

function Request:get_full_frame()
  if not self.op_code then error("Request#write() has no op_code attribute") end
  if not self.version then error("Request#write() has no version attribute") end

  if not self.built then
    self:build()
    self.built = true
  end

  local frameHeader = FrameHeader(self.version, self.flags, self.op_code, self.frameBody.len)
  local header = frameHeader:dump()
  local body = self.frameBody:dump()

  return header..body
end

--- StartupRequest
-- @section startup_request

local StartupRequest = Request:extend()

function StartupRequest:new()
  StartupRequest.super.new(self, op_codes.STARTUP)
end

function StartupRequest:build()
  self.frameBody:write_string_map({
    CQL_VERSION = CONSTS.CQL_VERSION
  })
end

--- QueryRequest
-- @section query_request

local QueryRequest = Request:extend()

function QueryRequest:new(query, params, options)
  self.query = query
  self.params = params
  self.options = options
  QueryRequest.super.new(self, op_codes.QUERY)
end

function QueryRequest:build()
  -- v2: <query>
  --      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  -- v3: <query>
  --      <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
  self.frameBody:write_long_string(self.query)
  self.frameBody:write_short(0x0001) -- @TODO support consistency_level
  self.frameBody:write_byte(0) -- @TODO support query flags
end

return {
  StartupRequest = StartupRequest,
  QueryRequest = QueryRequest
}
