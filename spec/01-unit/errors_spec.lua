local Errors = require "cassandra.errors"

describe("Errors", function()
  it("has error type constants", function()
    assert.truthy(Errors.t_cql)
    assert.truthy(Errors.t_timeout)
    assert.truthy(Errors.t_socket)
  end)

  describe("no_host()", function()
    it("accepts a string message", function()
      local err = Errors.no_host "Nothing worked as planned"
      assert.equal("Nothing worked as planned", err)
    end)
    it("accepts a table", function()
      local err = Errors.no_host {["abc"] = "DOWN", ["def"] = "DOWN"}
      -- can't be sure in which order will the table be iterated over
      assert.truthy(string.match(err, "all hosts tried for query failed%. %l%l%l: DOWN%. %l%l%l: DOWN%."))
    end)
  end)

  describe("socket()", function()
    it("accepts an address and the error from the socket", function()
      local err = Errors.socket("127.0.0.1", "closed")
      assert.equal("socket with peer '127.0.0.1' encountered error: closed", err)
    end)
  end)

  describe("shm()", function()
    it("accepts a string", function()
      local err = Errors.shm("cassandra", "no memory")
      assert.equal("shared dict 'cassandra' encountered error: no memory", err)
    end)
  end)

  describe("internal_driver()", function()
    it("accepts a string", function()
      local err = Errors.internal_driver("no details for host")
      assert.equal("internal driver error: no details for host", err)
    end)
  end)

  describe("options()", function()
    it("accepts a string", function()
      local err = Errors.options("must contain contact_points")
      assert.equal("option error: must contain contact_points", err)
    end)
  end)
end)
