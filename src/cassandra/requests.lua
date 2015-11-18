local bit = require "cassandra.utils.bit"
local types = require "cassandra.types"
local CONSTS = require "cassandra.constants"
local Object = require "cassandra.classic"
local Buffer = require "cassandra.buffer"
local frame_header = require "cassandra.types.frame_header"

local op_codes = frame_header.op_codes
local FrameHeader = frame_header.FrameHeader

local string_format = string.format

--- Query Flags
-- @section query_flags

local query_flags = {
  values = 0x01,
  skip_metadata = 0x02, -- not implemented
  page_size = 0x04,
  paging_state = 0x08,
  -- 0x09
  serial_consistency = 0x10,
  default_timestamp = 0x20,
  named_values = 0x40
}

--- Request
-- @section request

local Request = Object:extend()

function Request:new(op_code)
  self.version = nil
  self.flags = 0
  self.op_code = op_code
  self.frameBody = Buffer() -- no version yet at this point
  self.built = false

  Request.super.new(self)
end

function Request:set_version(version)
  self.version = version
  self.frameBody.version = version
end

function Request:build()
  error("Request:build() must be implemented")
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
  self.options = options or {}
  QueryRequest.super.new(self, op_codes.QUERY)
end

function QueryRequest:build()
  -- v2: <query>
  --      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  -- v3: <query>
  --      <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
  if self.options.consistency == nil then
    self.options.consistency = types.consistencies.one
  end

  local flags = 0x00
  local flags_buffer = Buffer(self.version)
  if self.params ~= nil then
    flags = bit.bor(flags, query_flags.values)
    flags_buffer:write_cql_values(self.params)
  end
  if self.options.page_size ~= nil then
    flags = bit.bor(flags, query_flags.page_size)
    flags_buffer:write_int(self.options.page_size)
  end
  if self.options.paging_state ~= nil then
    flags = bit.bor(flags, query_flags.paging_state)
    flags_buffer:write_bytes(self.options.paging_state)
  end
  if self.options.serial_consistency ~= nil then
    flags = bit.bor(flags, query_flags.serial_consistency)
    flags_buffer:write_short(self.options.serial_consistency)
  end

  self.frameBody:write_long_string(self.query)
  self.frameBody:write_short(self.options.consistency)
  self.frameBody:write_byte(flags)
  self.frameBody:write(flags_buffer:dump())
end

--- KeyspaceRequest
-- @section keyspace_request

local KeyspaceRequest = QueryRequest:extend()

function KeyspaceRequest:new(keyspace)
  local query = string_format([[USE "%s"]], keyspace)
  KeyspaceRequest.super.new(self, query)
end

return {
  StartupRequest = StartupRequest,
  QueryRequest = QueryRequest,
  KeyspaceRequest = KeyspaceRequest
}
