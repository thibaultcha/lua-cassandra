local helpers = require "spec.helpers"
local cassandra = require "cassandra"

local ca_path = helpers.ssl_path.."/cassandra.pem"

describe("SSL", function()
  setup(function()
    helpers.ccm_start {ssl = true, name = "ssl"}
  end)

  describe("host", function()
    it("does not connect without SSL enabled", function()
      local peer = assert(cassandra.new())
      local ok, err = peer:connect()
      assert.is_nil(ok)
      assert.equal("closed", err)
    end)
    it("connects with SSL", function()
      local peer = assert(cassandra.new {ssl = true})
      assert(peer:connect())
      local rows = assert(peer:execute "SELECT * FROM system.local")
      assert.equal(1, #rows)
    end)
    it("connects with SSL and verifying server certificate", function()
      local peer = assert(cassandra.new {
        ssl = true,
        verify = true,
        cafile = ca_path
      })
      assert(peer:connect())
      local rows = assert(peer:execute "SELECT * FROM system.local")
      assert.equal(1, #rows)
    end)
  end)
end)
