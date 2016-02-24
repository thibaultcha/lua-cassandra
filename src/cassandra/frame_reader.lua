local bit = require "cassandra.utils.bit"
local types = require "cassandra.types"
local Object = require "cassandra.utils.classic"
local Buffer = require "cassandra.buffer"
local Errors = require "cassandra.errors"
local OP_CODES = types.OP_CODES

--- CONST
-- @section constants

local RESULT_KINDS = {
  VOID = 0x01,
  ROWS = 0x02,
  SET_KEYSPACE = 0x03,
  PREPARED = 0x04,
  SCHEMA_CHANGE = 0x05
}

local ROWS_RESULT_FLAGS = {
  GLOBAL_TABLES_SPEC = 0x01,
  HAS_MORE_PAGES = 0x02,
  NO_METADATA = 0x04
}

--- ResultParser
-- @section result_parser

local function parse_metadata(buffer)
  local k_name, t_name, paging_state

  local flags = buffer:read_int()
  local columns_count = buffer:read_int()

  local has_more_pages = bit.btest(flags, ROWS_RESULT_FLAGS.HAS_MORE_PAGES)
  local has_global_table_spec = bit.btest(flags, ROWS_RESULT_FLAGS.GLOBAL_TABLES_SPEC)
  --local has_no_metadata = bit.btest(flags, ROWS_RESULT_FLAGS.NO_METADATA)

  if has_more_pages then
    paging_state = buffer:read_bytes()
  end

  if has_global_table_spec then
    k_name = buffer:read_string()
    t_name = buffer:read_string()
  end

  local columns = {}
  for _ = 1, columns_count do
    if not has_global_table_spec then
      k_name = buffer:read_string()
      t_name = buffer:read_string()
    end
    local col_name = buffer:read_string()
    local col_type = buffer:read_options()
    columns[#columns + 1] = {
      name = col_name,
      type = col_type,
      keysapce = k_name,
      table = t_name
    }
  end

  return {
    columns = columns,
    columns_count = columns_count,
    has_more_pages = has_more_pages,
    paging_state = paging_state
  }
end

local RESULT_PARSERS = {
  [RESULT_KINDS.VOID] = function()
    return {type = "VOID"}
  end,
  [RESULT_KINDS.ROWS] = function(buffer)
    local metadata = parse_metadata(buffer)
    local columns = metadata.columns
    local columns_count = metadata.columns_count
    local rows_count = buffer:read_int()

    local rows = {
      type = "ROWS",
      meta = {
        has_more_pages = metadata.has_more_pages,
        paging_state = metadata.paging_state
      }
    }
    for _ = 1, rows_count do
      local row = {}
      for i = 1, columns_count do
        row[columns[i].name] = buffer:read_cql_value(columns[i].type)
      end
      rows[#rows + 1] = row
    end

    return rows
  end,
  [RESULT_KINDS.SET_KEYSPACE] = function(buffer)
    return {
      type = "SET_KEYSPACE",
      keyspace = buffer:read_string()
    }
  end,
  [RESULT_KINDS.PREPARED] = function(buffer)
    local query_id = buffer:read_short_bytes()
    local metadata = parse_metadata(buffer)
    local result_metadata = parse_metadata(buffer)
    return {
      type = "PREPARED",
      query_id = query_id,
      meta = metadata,
      result = result_metadata
    }
  end,
  [RESULT_KINDS.SCHEMA_CHANGE] = function(buffer)
    return {
      type = "SCHEMA_CHANGE",
      change = buffer:read_string(),
      keyspace = buffer:read_string(),
      table = buffer:read_string()
    }
  end
}

--- FrameHeader
-- @section frameheader

local FrameReader = Object:extend()

function FrameReader:new(frameHeader, body_bytes)
  self.frameHeader = frameHeader
  self.frame_body = Buffer(frameHeader.version, body_bytes)
end

local function parse_error(frame_body)
  local code = frame_body:read_int()
  local message = frame_body:read_string()
  local code_translation = types.ERRORS_TRANSLATIONS[code]
  return "["..code_translation.."] "..message, Errors.t_cql, code
end

local function parse_ready()
  return {ready = true}
end

local function parse_result(frame_body)
  local result_kind = frame_body:read_int()
  local parser = RESULT_PARSERS[result_kind]
  return parser(frame_body)
end

local function parse_authenticate(frame_body)
  local class_name = frame_body:read_string()
  return {must_authenticate = true, class_name = class_name}
end

local function parse_auth_success(frame_body)
  local token = frame_body:read_bytes()
  return {authenticated = true, token = token}
end

--- Decode a response frame
function FrameReader:parse()
  local op_code = self.frameHeader.op_code
  if op_code == nil then
    error("frame header has no op_code")
  end

  -- Parse frame depending on op_code
  if op_code == OP_CODES.ERROR then
    return nil, parse_error(self.frame_body)
  elseif op_code == OP_CODES.READY then
    return parse_ready(self.frame_body)
  elseif op_code == OP_CODES.AUTHENTICATE then
    return parse_authenticate(self.frame_body)
  elseif op_code == OP_CODES.AUTH_SUCCESS then
    return parse_auth_success(self.frame_body)
  elseif op_code == OP_CODES.RESULT then
    return parse_result(self.frame_body)
  end
end

return FrameReader
