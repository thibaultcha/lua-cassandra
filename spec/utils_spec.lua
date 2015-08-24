local utils = require "cassandra.utils"
local is_array = utils.is_array

describe("Utils", function()
  describe("is_array()", function()
    it("should return -1 when is not an array", function()
      assert.equal(-1, is_array({foo = "bar", bar = "foo"}))
      assert.equal(-1, is_array({[1] = "bar", bar = "foo"}))
      assert.equal(-1, is_array({foo = "bar", [2] = "foo"}))

      assert.equal(-1, is_array({[1] = "bar", [4] = "foo"}))
      assert.equal(-1, is_array({[1] = "bar", [40] = "foo"}))
      assert.equal(-1, is_array({[5] = "bar", [6] = "foo"}))
    end)
    it("should return 0 when the table is empty", function()
      assert.equal(0, is_array({}))
    end)
    it("should return the max index when is an array", function()
      assert.equal(1, is_array({"foo"}))
      assert.equal(2, is_array({"foo", "bar"}))
      assert.equal(3, is_array({"foo", "bar", "baz"}))
    end)
  end)
end)
