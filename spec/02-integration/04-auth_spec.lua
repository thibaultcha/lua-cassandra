local utils = require "spec.spec_utils"
local cassandra = require "cassandra"
local Cluster = require "cassandra.cluster"

local ca_path = utils.ssl_path.."/cassandra.pem"

describe("plain_text auth provider", function()
  setup(function()
    utils.ccm_start(1, {
      ssl = true,
      pwd_auth = true,
      name = "auth"
    })
  end)

  describe("host", function()
    it("complains if no auth provider was configured", function()
      local peer = assert(cassandra.new {
        ssl = true,
        verify = true,
        cafile = ca_path
      })
      local ok, err = peer:connect()
      assert.equal("authentication required", err)
      assert.is_nil(ok)
    end)
    it("is refused if credentials are invalid", function()
      local peer = assert(cassandra.new {
        ssl = true,
        verify = true,
        cafile = ca_path,
        auth = cassandra.auth_providers.plain_text("cassandra", "foo")
      })
      local ok, err = peer:connect()
      assert.equal("[Bad credentials] Username and/or password are incorrect", err)
      assert.is_nil(ok)
    end)
    it("authenticates with valid credentials", function()
      local peer = assert(cassandra.new {
        ssl = true,
        verify = true,
        cafile = ca_path,
        auth = cassandra.auth_providers.plain_text("cassandra", "cassandra")
      })
      assert(peer:connect())
    end)
  end)

  describe("cluster", function()
    it("complains if no auth provider was configured", function()
      local cluster = assert(Cluster.new {
        ssl = true,
        verify = true,
        cafile = ca_path
      })
      local ok, err = cluster:refresh()
      assert.equal("all hosts tried for query failed. 127.0.0.1: authentication required", err)
      assert.is_nil(ok)
    end)
    it("is refused if credentials are invalid", function()
      local cluster = assert(Cluster.new {
        ssl = true,
        verify = true,
        cafile = ca_path,
        auth = cassandra.auth_providers.plain_text("cassandra", "foo")
      })
      local ok, err = cluster:refresh()
      assert.equal("all hosts tried for query failed. 127.0.0.1: [Bad credentials] Username and/or password are incorrect", err)
      assert.is_nil(ok)
    end)
    it("authenticates with valid credentials", function()
      local cluster = assert(Cluster.new {
        ssl = true,
        verify = true,
        cafile = ca_path,
        auth = cassandra.auth_providers.plain_text("cassandra", "cassandra")
      })
      assert(cluster:refresh())
    end)
  end)
end)
