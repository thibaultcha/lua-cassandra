local utils = require "spec.spec_utils"
local host = require "cassandra.host"
local Cluster = require "cassandra.cluster"

local ca_path = utils.ssl_path.."/cassandra.pem"

describe("SSL", function()
  setup(function()
    utils.ccm_start(1, {ssl = true, name = "ssl"})
  end)

  describe("host", function()
    it("does not connect without SSL enabled", function()
      local peer = assert(host.new())
      local ok, err = peer:connect()
      assert.is_nil(ok)
      assert.equal("closed", err)
    end)
    it("connects with SSL", function()
      local peer = assert(host.new {ssl = true})
      assert(peer:connect())
      local rows = assert(peer:execute "SELECT * FROM system.local")
      assert.equal(1, #rows)
    end)
    it("connects with SSL and verifying server certificate", function()
      local peer = assert(host.new {
        ssl = true,
        verify = true,
        cafile = ca_path
      })
      assert(peer:connect())
      local rows = assert(peer:execute "SELECT * FROM system.local")
      assert.equal(1, #rows)
    end)
  end)

  describe("cluster", function()
    it("does not connect without SSL enabled", function()
      local cluster = assert(Cluster.new())
      local ok, err = cluster:refresh()
      assert.is_nil(ok)
      assert.equal("all hosts tried for query failed. 127.0.0.1: host seems unhealthy: closed", err)
    end)
    it("connects with SSL", function()
      local cluster = assert(Cluster.new {ssl = true})
      local rows = assert(cluster:execute "SELECT * FROM system.local")
      assert.equal(1, #rows)
    end)
    it("connects with SSL and verifying server certificate", function()
      local cluster = assert(Cluster.new {
        ssl = true,
        verify = true,
        cafile = ca_path
      })
      local rows = assert(cluster:execute "SELECT * FROM system.local")
      assert.equal(1, #rows)
    end)
  end)
end)
