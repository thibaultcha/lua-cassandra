local utils = require "cassandra.utils.table"
local bit = require "cassandra.utils.bit"
local Buffer = require "cassandra.buffer"

--- CONST
-- @section constants

local VERSION_CODES = {
  [2] = {
    REQUEST = 0x02,
    RESPONSE = 0x82
  },
  [3] = {
    REQUEST = 0x03,
    RESPONSE = 0x83
  }
}

setmetatable(VERSION_CODES, utils.const_mt)

local FLAGS = {
  COMPRESSION = 0x01, -- not implemented
  TRACING = 0x02
}

-- when we'll support protocol v4, other
-- flags will be added.
-- setmetatable(FLAGS, utils.const_mt)

local OP_CODES = {
  ERROR = 0x00,
  STARTUP = 0x01,
  READY = 0x02,
  AUTHENTICATE = 0x03,
  OPTIONS = 0x05,
  SUPPORTED = 0x06,
  QUERY = 0x07,
  RESULT = 0x08,
  PREPARE = 0x09,
  EXECUTE = 0x0A,
  REGISTER = 0x0B,
  EVENT = 0x0C,
  BATCH = 0x0D,
  AUTH_CHALLENGE = 0x0E,
  AUTH_RESPONSE = 0x0F,
  AUTH_SUCCESS = 0x10
}

--- FrameHeader
-- @section FrameHeader

local FrameHeader = Buffer:extend()

function FrameHeader:new(version, flags, op_code, body_length)
  self.flags = flags and flags or 0
  self.op_code = op_code
  self.stream_id = 0 -- @TODO support streaming
  self.body_length = body_length

  self.super.new(self, version)
end

function FrameHeader:dump()
  FrameHeader.super.write_byte(self, VERSION_CODES:get("REQUEST", self.version))
  FrameHeader.super.write_byte(self, self.flags) -- @TODO find a more secure way

  if self.version < 3 then
    FrameHeader.super.write_byte(self, self.stream_id)
  else
    FrameHeader.super.write_short(self, self.stream_id)
  end

  FrameHeader.super.write_byte(self, self.op_code) -- @TODO find a more secure way
  FrameHeader.super.write_int(self, self.body_length)

  return FrameHeader.super.dump(self)
end

function FrameHeader.version_from_byte(byte)
  local buf = Buffer(nil, byte)
  return bit.band(buf:read_byte(), 0x7F)
end

function FrameHeader.size_from_byte(version_byte)
  if FrameHeader.version_from_byte(version_byte) < 3 then
    return 8
  else
    return 9
  end
end

function FrameHeader.from_raw_bytes(version_byte, raw_bytes)
  local version = FrameHeader.version_from_byte(version_byte)
  local buffer = Buffer(version, raw_bytes)
  local flags = buffer:read_byte()

  local stream_id
  if version < 3 then
    stream_id = buffer:read_byte()
  else
    stream_id = buffer:read_short()
  end

  local op_code = buffer:read_byte()
  local body_length = buffer:read_int()

  return FrameHeader(version, flags, op_code, body_length)
end

return {
  op_codes = OP_CODES,
  flags = FLAGS,
  FrameHeader = FrameHeader
}
