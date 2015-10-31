local CONSTS = require "cassandra.consts"
local requests = require "cassandra.requests"
local Buffer = require "cassandra.buffer"
local frame_header = require "cassandra.types.frame_header"

local op_codes = frame_header.op_codes

describe("Requests", function()
  local Request = requests.Request
  describe("Request", function()
    it("should write its own frame", function()
      local buffer = Buffer()
      buffer:write_byte(0x03)
      buffer:write_byte(0) -- flags
      buffer:write_byte(0) -- stream id
      buffer:write_byte(op_codes.STARTUP)
      buffer:write_integer(0) -- body length

      local req = Request({op_code = op_codes.STARTUP})
      assert.equal(buffer:write(), req:write())
    end)
    it("should proxy all writer functions", function()
      local buffer = Buffer()
      buffer:write_byte(0x03)
      buffer:write_byte(0) -- flags
      buffer:write_byte(0) -- stream id
      buffer:write_byte(op_codes.STARTUP)
      buffer:write_integer(22) -- body length
      buffer:write_string_map({CQL_VERSION = "3.0.0"})

      local req = Request({op_code = op_codes.STARTUP})
      assert.has_no_errors(function()
        req:write_string_map({CQL_VERSION = "3.0.0"})
      end)

      assert.equal(buffer:write(), req:write())
    end)
  end)
  describe("StartupRequest", function()
    it("should write a startup request", function()
      local req = Request({op_code = op_codes.STARTUP})
      req:write_string_map({CQL_VERSION = "3.0.0"})

      local startup = requests.StartupRequest()

      assert.equal(req:write(), startup:write())
    end)
  end)
end)
