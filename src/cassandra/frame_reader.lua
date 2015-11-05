local Object = require "cassandra.classic"
local Buffer = require "cassandra.buffer"
local errors = require "cassandra.errors"
local frame_header = require "cassandra.types.frame_header"
local bit = require "cassandra.utils.bit"
local op_codes = frame_header.op_codes

--- CONST
-- @section constants

local ERRORS = {
  SERVER = 0x0000,
  PROTOCOL = 0x000A,
  BAD_CREDENTIALS = 0x0100,
  UNAVAILABLE_EXCEPTION = 0x1000,
  OVERLOADED = 0x1001,
  IS_BOOTSTRAPPING = 0x1002,
  TRUNCATE_ERROR = 0x1003,
  WRITE_TIMEOUT = 0x1100,
  READ_TIMEOUT = 0x1200,
  SYNTAX_ERROR = 0x2000,
  UNAUTHORIZED = 0x2100,
  INVALID = 0x2200,
  CONFIG_ERROR = 0x2300,
  ALREADY_EXISTS = 0x2400,
  UNPREPARED = 0x2500
}

local ERRORS_TRANSLATION = {
  [ERRORS.SERVER] = "Server error",
  [ERRORS.PROTOCOL] = "Protocol error",
  [ERRORS.BAD_CREDENTIALS] = "Bad credentials",
  [ERRORS.UNAVAILABLE_EXCEPTION] = "Unavailable exception",
  [ERRORS.OVERLOADED] = "Overloaded",
  [ERRORS.IS_BOOTSTRAPPING] = "Is bootstrapping",
  [ERRORS.TRUNCATE_ERROR] = "Truncate error",
  [ERRORS.WRITE_TIMEOUT] = "Write timeout",
  [ERRORS.READ_TIMEOUT] = "Read timeout",
  [ERRORS.SYNTAX_ERROR] = "Syntaxe rror",
  [ERRORS.UNAUTHORIZED] = "Unauthorized",
  [ERRORS.INVALID] = "Invalid",
  [ERRORS.CONFIG_ERROR] = "Config error",
  [ERRORS.ALREADY_EXISTS] = "Already exists",
  [ERRORS.UNPREPARED] = "Unprepared"
}

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
  local k_name, t_name

  local flags = buffer:read_int()
  local columns_count = buffer:read_int()

  local has_more_pages = bit.btest(flags, ROWS_RESULT_FLAGS.HAS_MORE_PAGES)
  local has_global_table_spec = bit.btest(flags, ROWS_RESULT_FLAGS.GLOBAL_TABLES_SPEC)
  local has_no_metadata = bit.btest(flags, ROWS_RESULT_FLAGS.NO_METADATA)

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
    local col_type = buffer:read_options() -- {type_id = ...[, value_type_id = ...]}
    columns[#columns + 1] = {
      name = col_name,
      type = col_type,
      keysapce = k_name,
      table = t_name
    }
  end

  return {
    columns = columns,
    columns_count = columns_count
  }
end

local RESULT_PARSERS = {
  [RESULT_KINDS.ROWS] = function(buffer)
    local metadata = parse_metadata(buffer)
    local columns = metadata.columns
    local columns_count = metadata.columns_count
    local rows_count = buffer:read_int()

    local rows = {
      type = "ROWS"
    }
    for _ = 1, rows_count do
      local row = {}
      for i = 1, columns_count do
        --print("reading column "..columns[i].name)
        local value = buffer:read_cql_value(columns[i].type)
        local inspect = require "inspect"
        --print("column "..columns[i].name.." = "..inspect(value))
        row[columns[i].name] = value
      end
      rows[#rows + 1] = row
    end

    return rows
  end
}

--- FrameHeader
-- @section frameheader

local FrameReader = Object:extend()

function FrameReader:new(frameHeader, body_bytes)
  self.frameHeader = frameHeader
  self.frameBody = Buffer(frameHeader.version, body_bytes)
end

local function parse_error(frameBody)
  local code = frameBody:read_int()
  local message = frameBody:read_string()
  local code_translation = ERRORS_TRANSLATION[code]
  return errors.ResponseError(code, code_translation, message)
end

local function parse_ready()
  return {ready = true}
end

local function parse_result(frameBody)
  local result_kind = frameBody:read_int()
  local parser = RESULT_PARSERS[result_kind]
  return parser(frameBody)
end

--- Decode a response frame
function FrameReader:parse()
  local op_code = self.frameHeader.op_code
  if op_code == nil then
    error("frame header has no op_code")
  end

  -- Parse frame depending on op_code
  if op_code == op_codes.ERROR then
    return nil, parse_error(self.frameBody)
  elseif op_code == op_codes.READY then
    return parse_ready(self.frameBody)
  elseif op_code == op_codes.RESULT then
    return parse_result(self.frameBody)
  end
end

return {
  FrameReader = FrameReader,
  errors = ERRORS
}
