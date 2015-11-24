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

--- FrameHeader
-- @section FrameHeader

local FrameHeader = Buffer:extend()

function FrameHeader:new(version, flags, op_code, body_length, stream_id)
  self.flags = flags and flags or 0
  self.op_code = op_code
  self.stream_id = stream_id or 0
  self.body_length = body_length

  self.super.new(self, version)
end

function FrameHeader:dump()
  FrameHeader.super.write_byte(self, VERSION_CODES[self.version].REQUEST)
  FrameHeader.super.write_byte(self, self.flags)

  if self.version < 3 then
    FrameHeader.super.write_byte(self, self.stream_id)
  else
    FrameHeader.super.write_short(self, self.stream_id)
  end

  FrameHeader.super.write_byte(self, self.op_code)
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

  return FrameHeader(version, flags, op_code, body_length, stream_id)
end

return FrameHeader
