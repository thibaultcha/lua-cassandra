local types = require "cassandra.types"
local Buffer = require "cassandra.buffer"
local requests = require "cassandra.requests"

describe("Requests", function()
  describe("StartupRequest", function()
    it("should write a startup request", function()
      local startup = requests.StartupRequest()
      startup:set_version(3)

      local full_buffer = Buffer(3, startup:get_full_frame())

      assert.equal(0x03, full_buffer:read_byte())
      assert.equal(0, full_buffer:read_byte())
      assert.equal(0, full_buffer:read_short())
      assert.equal(types.OP_CODES.STARTUP, full_buffer:read_byte())
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
      assert.equal(types.OP_CODES.STARTUP, full_buffer:read_byte())
      assert.equal(22, full_buffer:read_int())
      assert.same({CQL_VERSION = "3.0.0"}, full_buffer:read_string_map())
    end)
  end)
end)
