local Client = require "cassandra.client"

local FAKE_CLUSTER = {"0.0.0.1", "0.0.0.2", "0.0.0.3"}
local ONE_NODE_CLUSTER = {"127.0.0.1"}

describe("Client", function()
  it("should be instanciable", function()
    assert.has_no_errors(function()
      local client = Client({contact_points = FAKE_CLUSTER})
      assert.equal(false, client.connected)
    end)
  end)
  describe("#execute()", function()
    it("should return error if no host is available", function()
      local client = Client({contact_points = FAKE_CLUSTER})
      local err = client:execute()
      assert.truthy(err)
      assert.equal("NoHostAvailableError", err.type)
    end)
    it("TODO", function()
      local client = Client({contact_points = ONE_NODE_CLUSTER})
      local err = client:execute()
    end)
  end)
end)
