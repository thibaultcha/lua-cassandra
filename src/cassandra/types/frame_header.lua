local utils = require "cassandra.utils"
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

setmetatable(FLAGS, utils.const_mt)

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

setmetatable(OP_CODES, utils.const_mt)

--- FrameHeader
-- @section FrameHeader

local FrameHeader = Buffer:extend()

function FrameHeader:new(version, flags, op_code, body_length)
  self.flags = flags and flags or 0
  self.op_code = op_code
  self.body_length = body_length

  self.super.new(self, nil, version)
end

function FrameHeader:write()
  self.super.write_byte(self, VERSION_CODES[self.version].REQUEST)
  self.super.write_byte(self, self.flags) -- @TODO make sure to expose flags to the client or find a more secure way
  self.super.write_byte(self, 0) -- @TODO support streaming
  self.super.write_byte(self, self.op_code) -- @TODO make sure to expose op_codes to the client or find a more secure way
  self.super.write_integer(self, self.body_length)

  return self.super.write(self)
end

return {
  op_codes = OP_CODES,
  flags = FLAGS,
  FrameHeader = FrameHeader
}
