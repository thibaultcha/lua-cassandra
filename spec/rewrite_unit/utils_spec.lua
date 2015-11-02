local utils = require "cassandra.utils"

describe("utils", function()
  describe("const_mt", function()

    local VERSION_CODES = {
      [2] = {
        REQUEST = 20,
        RESPONSE = 21,
        SOME_V2 = 2222
      },
      [3] = {
        REQUEST = 30,
        RESPONSE = 31,
        SOME_V3_ONLY = 3333
      }
    }
    setmetatable(VERSION_CODES, utils.const_mt)

    local FLAGS = {
      COMPRESSION = 1,
      TRACING = 2,
      [4] = {
        CUSTOM_PAYLOAD = 4
      }
    }
    setmetatable(FLAGS, utils.const_mt)

    describe("#get()", function()
      it("should get most recent version of a constant", function()
        assert.equal(30, VERSION_CODES:get("REQUEST"))
        assert.equal(31, VERSION_CODES:get("RESPONSE"))
        assert.equal(3333, VERSION_CODES:get("SOME_V3_ONLY"))
        assert.equal(2222, VERSION_CODES:get("SOME_V2"))
      end)
      it("should get constant from the root", function()
        assert.equal(1, FLAGS:get("COMPRESSION"))
        assert.equal(2, FLAGS:get("TRACING"))
      end)
      it("should accept a version parameter for which version to look into", function()
        assert.equal(4, FLAGS:get("CUSTOM_PAYLOAD", 4))
        assert.equal(20, VERSION_CODES:get("REQUEST", 2))
        assert.equal(21, VERSION_CODES:get("RESPONSE", 2))
      end)
    end)

  end)
end)
