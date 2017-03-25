local helpers = require "spec.helpers"
local cassandra = require "cassandra"

local ca_path = helpers.ssl_path.."/cassandra.pem"

describe("plain_text auth provider", function()
  setup(function()
    helpers.ccm_start {
      ssl = true,
      pwd_auth = true,
      name = "auth"
    }
  end)

  describe("new()", function()
    it("requires 'username' to be a string", function()
      assert.error_matches(function()
        cassandra.auth_providers.plain_text()
      end, "arg #1 must be a string (username)", nil, true)
    end)
    it("requires 'password' to be a string", function()
      assert.error_matches(function()
        cassandra.auth_providers.plain_text("")
      end, "arg #2 must be a string (password)", nil, true)
    end)
  end)

  describe("host", function()
    it("complains if no auth provider was configured", function()
      local peer = assert(cassandra.new {
        ssl = true,
        verify = true,
        cafile = ca_path
      })
      local ok, err = peer:connect()
      assert.is_nil(ok)
      assert.equal("authentication required", err)
    end)
    it("is refused if credentials are invalid", function()
      local peer = assert(cassandra.new {
        ssl = true,
        verify = true,
        cafile = ca_path,
        auth = cassandra.auth_providers.plain_text("cassandra", "foo")
      })
      local ok, err = peer:connect()
      assert.is_nil(ok)

      if helpers.cassandra_version_num >= 31000 then
        assert.equal("[Bad credentials] Provided username cassandra and/or password are incorrect", err)

      else
        assert.equal("[Bad credentials] Username and/or password are incorrect", err)
      end
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
end)
