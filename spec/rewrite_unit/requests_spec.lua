local CONSTS = require "cassandra.consts"
local requests = require "cassandra.requests"
local Buffer = require "cassandra.buffer"
local frame_header = require "cassandra.types.frame_header"

local op_codes = frame_header.op_codes

describe("Requests", function()
  local Request = requests.Request
  describe("Request", function()
    it("should write its own frame", function()
      local buffer = Buffer(nil, 3)
      buffer:write_byte(0x03)
      buffer:write_byte(0) -- flags
      buffer:write_short(0) -- stream id
      buffer:write_byte(op_codes.STARTUP)
      buffer:write_integer(0) -- body length

      local req = Request({version = 3, op_code = op_codes.STARTUP})
      assert.equal(buffer:write(), req:get_full_frame())
    end)
    it("should proxy all writer functions", function()
      local buffer = Buffer(nil, 3)
      buffer:write_byte(0x03)
      buffer:write_byte(0) -- flags
      buffer:write_short(0) -- stream id
      buffer:write_byte(op_codes.STARTUP)
      buffer:write_integer(22) -- body length
      buffer:write_string_map({CQL_VERSION = "3.0.0"})

      local req = Request({version = 3, op_code = op_codes.STARTUP})
      assert.has_no_errors(function()
        req:write_string_map({CQL_VERSION = "3.0.0"})
      end)

      assert.equal(buffer:write(), req:get_full_frame())
    end)
  end)
  describe("StartupRequest", function()
    it("should write a startup request", function()
      -- Raw request
      local req = Request({version = 3, op_code = op_codes.STARTUP})
      req:write_string_map({CQL_VERSION = "3.0.0"})
      local full_buffer = Buffer(req:get_full_frame(), 3)

      -- Startup sugar request
      local startup = requests.StartupRequest({version = 3})

      assert.equal(0x03, full_buffer:read_byte())
      assert.equal(0, full_buffer:read_byte())
      assert.equal(0, full_buffer:read_short())
      assert.equal(op_codes.STARTUP, full_buffer:read_byte())
      assert.equal(22, full_buffer:read_integer())
      assert.same({CQL_VERSION = "3.0.0"}, full_buffer:read_string_map())
      assert.equal(full_buffer:write(), req:get_full_frame())
      assert.equal(full_buffer:write(), startup:get_full_frame())
    end)
  end)
  describe("Protocol versions", function()
    it("should support other versions of the protocol", function()
      -- Raw request
      local req = Request({version = 2, op_code = op_codes.STARTUP})
      req:write_string_map({CQL_VERSION = "3.0.0"})
      local full_buffer = Buffer(req:get_full_frame(), 2)

      -- Startup sugar request
      local startup = requests.StartupRequest({version = 2})

      assert.equal(0x02, full_buffer:read_byte())
      assert.equal(0, full_buffer:read_byte())
      assert.equal(0, full_buffer:read_byte())
      assert.equal(op_codes.STARTUP, full_buffer:read_byte())
      assert.equal(22, full_buffer:read_integer())
      assert.same({CQL_VERSION = "3.0.0"}, full_buffer:read_string_map())
      assert.equal(full_buffer:write(), req:get_full_frame())
      assert.equal(full_buffer:write(), startup:get_full_frame())
    end)
  end)
end)
