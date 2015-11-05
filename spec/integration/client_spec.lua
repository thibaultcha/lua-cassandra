local Client = require "cassandra.client"
local t_utils = require "cassandra.utils.table"

local FAKE_CLUSTER = {"0.0.0.1", "0.0.0.2", "0.0.0.3"}
local contact_points_2_0 = {"127.0.0.1:9001"}
local contact_points_2_1 = {"127.0.0.1:9101"}

local function client_factory(opts)
  t_utils.extend_table({print_log_level = "DEBUG"}, opts)
  return Client(opts)
end

describe("Client", function()
  it("should be instanciable", function()
    assert.has_no_errors(function()
      local client = client_factory({contact_points = FAKE_CLUSTER})
      assert.equal(false, client.connected)
    end)
  end)
  describe("#execute()", function()
    it("should return error if no host is available", function()
      local client = client_factory({contact_points = FAKE_CLUSTER})
      local err = client:execute()
      assert.truthy(err)
      assert.equal("NoHostAvailableError", err.type)
    end)
    it("should connect to a cluster", function()
      local client = client_factory({contact_points = contact_points_2_1})
      local err = client:execute()
      assert.falsy(err)
    end)
    it("should retrieve cluster information when connecting", function()
      local client = client_factory({contact_points = contact_points_2_1})
      local err = client:execute()
      assert.falsy(err)
    end)
  end)
  describe("binary protocol downgrade", function()
    it("should downgrade the protocol version if the node does not support the most recent one", function()
      local client = client_factory({contact_points = contact_points_2_0})
      local err = client:execute()
      assert.falsy(err)
    end)
  end)
end)
