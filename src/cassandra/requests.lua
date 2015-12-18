local bit = require "cassandra.utils.bit"
local types = require "cassandra.types"
local Object = require "cassandra.utils.classic"
local Buffer = require "cassandra.buffer"
local FrameHeader = require "cassandra.types.frame_header"

local OP_CODES = types.OP_CODES
local string_byte = string.byte
local string_format = string.format
local unpack
if _VERSION == "Lua 5.3" then
  unpack = table.unpack
else
  unpack = _G.unpack
end

local CQL_VERSION = "3.0.0"

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
  self.version = nil -- no version yet at this point
  self.flags = 0
  self.op_code = op_code
  self.frame_body = Buffer() -- no version yet at this point
  self.built = false

  Request.super.new(self)
end

function Request:set_version(version)
  self.version = version
  self.frame_body.version = version
end

function Request:build()
  error("Request:build() must be implemented")
end

function Request:get_full_frame()
  if not self.op_code then error("Request#get_full_frame() has no op_code attribute") end
  if not self.version then error("Request#get_full_frame() has no version attribute") end

  if not self.built then
    self:build()
    self.built = true
  end

  local frame_header = FrameHeader(self.version, self.flags, self.op_code, self.frame_body.len)
  local header = frame_header:dump()
  local body = self.frame_body:dump()

  return header..body
end

--- StartupRequest
-- @section startup_request

local StartupRequest = Request:extend()

function StartupRequest:new()
  StartupRequest.super.new(self, OP_CODES.STARTUP)
end

function StartupRequest:build()
  self.frame_body:write_string_map({
    CQL_VERSION = CQL_VERSION
  })
end

--- QueryRequest
-- @section query_request

local function build_request_parameters(frame_body, version, params, options)
  -- v2: <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  -- v3: <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]

  if options.consistency == nil then
    options.consistency = types.consistencies.one
  end

  local flags = 0x00
  local flags_buffer = Buffer(version)
  if params ~= nil then
    flags = bit.bor(flags, query_flags.values)
    flags_buffer:write_cql_values(params)
  end
  if options.page_size ~= nil then
    flags = bit.bor(flags, query_flags.page_size)
    flags_buffer:write_int(options.page_size)
  end
  if options.paging_state ~= nil then
    flags = bit.bor(flags, query_flags.paging_state)
    flags_buffer:write_bytes(options.paging_state)
  end
  if options.serial_consistency ~= nil then
    flags = bit.bor(flags, query_flags.serial_consistency)
    flags_buffer:write_short(options.serial_consistency)
  end

  frame_body:write_short(options.consistency)
  frame_body:write_byte(flags)
  frame_body:write(flags_buffer:dump())
end

local QueryRequest = Request:extend()

function QueryRequest:new(query, params, options)
  self.query = query
  self.params = params
  self.options = options or {}
  QueryRequest.super.new(self, OP_CODES.QUERY)
end

function QueryRequest:build()
  -- v2: <query>
  --      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  -- v3: <query>
  --      <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]

  self.frame_body:write_long_string(self.query)
  build_request_parameters(self.frame_body, self.version, self.params, self.options)
end

--- KeyspaceRequest
-- @section keyspace_request

local KeyspaceRequest = QueryRequest:extend()

function KeyspaceRequest:new(keyspace)
  local query = string_format([[USE "%s"]], keyspace)
  KeyspaceRequest.super.new(self, query)
end

--- PrepareRequest
-- @section prepare_request

local PrepareRequest = Request:extend()

function PrepareRequest:new(query)
  self.query = query
  QueryRequest.super.new(self, OP_CODES.PREPARE)
end

function PrepareRequest:build()
  self.frame_body:write_long_string(self.query)
end

--- ExecutePreparedRequest
-- @section execute_prepared_request

local ExecutePreparedRequest = Request:extend()

function ExecutePreparedRequest:new(query_id, query, params, options)
  self.query_id = query_id
  self.query = query
  self.params = params
  self.options = options
  ExecutePreparedRequest.super.new(self, OP_CODES.EXECUTE)
end

function ExecutePreparedRequest:build()
  -- v2: <queryId>
  --      <consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
  -- v3: <queryId>
  --      <consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]

  self.frame_body:write_short_bytes(self.query_id)
  build_request_parameters(self.frame_body, self.version, self.params, self.options)
end

function ExecutePreparedRequest:hex_query_id()
  return bit.tohex(string_byte(self.query_id))
end

--- BatchRequest
-- @section batch_request

local BatchRequest = Request:extend()

function BatchRequest:new(queries, options)
  self.queries = queries
  self.options = options
  self.type = options.logged and 0 or 1
  self.type = options.counter and 2 or self.type
  BatchRequest.super.new(self, OP_CODES.BATCH)
end

function BatchRequest:build()
  -- v2: <type><n><query_1>...<query_n><consistency>
  -- v3: <type><n><query_1>...<query_n><consistency><flags>[<serial_consistency>][<timestamp>]

  self.frame_body:write_byte(self.type)
  self.frame_body:write_short(#self.queries)

  for _, q in ipairs(self.queries) do
    local query, args = unpack(q)
    if q.query_id ~= nil then
      self.frame_body:write_byte(1)
      self.frame_body:write_short_bytes(q.query_id)
    else
      self.frame_body:write_byte(0)
      self.frame_body:write_long_string(query)
    end
    if args ~= nil then
      self.frame_body:write_cql_values(args)
    else
      self.frame_body:write_short(0)
    end
  end

  self.frame_body:write_short(self.options.consistency)

  if self.version > 2 then
    local flags = 0x00
    local flags_buffer = Buffer(self.version)
    if self.options.serial_consistency ~= nil then
      flags = bit.bor(flags, query_flags.serial_consistency)
      flags_buffer:write_short(self.options.serial_consistency)
    end
    if self.options.timestamp ~= nil then
      flags = bit.bor(flags, query_flags.default_timestamp)
      flags_buffer:write_long(self.options.timestamp)
    end
    self.frame_body:write_byte(flags)
    self.frame_body:write(flags_buffer:dump())
  end
end

--- AuthResponse
-- @section auth_response

local AuthResponse = Request:extend()

function AuthResponse:new(token)
  self.token = token
  AuthResponse.super.new(self, OP_CODES.AUTH_RESPONSE)
end

function AuthResponse:build()
  self.frame_body:write_bytes(self.token)
end

return {
  QueryRequest = QueryRequest,
  StartupRequest = StartupRequest,
  PrepareRequest = PrepareRequest,
  KeyspaceRequest = KeyspaceRequest,
  ExecutePreparedRequest = ExecutePreparedRequest,
  BatchRequest = BatchRequest,
  AuthResponse = AuthResponse
}
