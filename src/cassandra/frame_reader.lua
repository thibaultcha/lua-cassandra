local Buffer = require "cassandra.buffer"
local errors = require "cassandra.errors"
local frame_header = require "cassandra.types.frame_header"
local op_codes = frame_header.op_codes
local bit = require "bit"

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

--- FrameHeader
-- @section frameheader

local FrameReader = Buffer:extend()

function FrameReader:new(frameHeader, raw_bytes)
  self.frameHeader = frameHeader

  FrameReader.super.new(self, raw_bytes, frameHeader.version)
end

local function read_frame(self)

end

local function parse_error(self)
  local code = FrameReader.super.read_integer(self)
  local message = FrameReader.super.read_string(self)
  local code_translation = ERRORS_TRANSLATION[code]
  return errors.ResponseError(code_translation, message)
end

local function parse_ready(self)
  return {ready = true}
end

--- Decode a response frame
function FrameReader:read()
  if self.frameHeader.op_code == nil then
    error("frame header has no op_code")
  end

  local op_code = self.frameHeader.op_code

  -- Parse frame depending on op_code
  if op_code == op_codes.ERROR then
    return nil, parse_error(self)
  elseif op_code == op_codes.READY then
    return parse_ready(self)
  end
end

return FrameReader
