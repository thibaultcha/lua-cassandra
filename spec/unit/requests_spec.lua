local CONSTS = require "cassandra.consts"
local requests = require "cassandra.requests"
local Buffer = require "cassandra.buffer"
local frame_header = require "cassandra.types.frame_header"

local op_codes = frame_header.op_codes

describe("Requests", function()
  describe("StartupRequest", function()
    it("should write a startup request", function()
      local startup = requests.StartupRequest()
      startup:set_version(3)

      local full_buffer = Buffer(3, startup:get_full_frame())

      assert.equal(0x03, full_buffer:read_byte())
      assert.equal(0, full_buffer:read_byte())
      assert.equal(0, full_buffer:read_short())
      assert.equal(op_codes.STARTUP, full_buffer:read_byte())
      assert.equal(22, full_buffer:read_int())
      assert.same({CQL_VERSION = "3.0.0"}, full_buffer:read_string_map())
    end)
  end)
  describe("Protocol versions", function()
    it("should support other versions of the protocol", function()
      local startup = requests.StartupRequest()
      startup:set_version(2)

      local full_buffer = Buffer(2, startup:get_full_frame())

      assert.equal(0x02, full_buffer:read_byte())
      assert.equal(0, full_buffer:read_byte())
      assert.equal(0, full_buffer:read_byte())
      assert.equal(op_codes.STARTUP, full_buffer:read_byte())
      assert.equal(22, full_buffer:read_int())
      assert.same({CQL_VERSION = "3.0.0"}, full_buffer:read_string_map())
    end)
  end)
end)
