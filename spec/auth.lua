--------
-- To run this test suite, edit your cassandra.yaml file and
-- change the `authenticator` property to `PasswordAuthenticator`.
-- `cassandra:cassandra` is the default root user.

local cassandra = require "cassandra"
local PasswordAuthenticator = require "cassandra.authenticators.PasswordAuthenticator"

describe("PasswordAuthenticator", function()
  it("should instanciate a PasswordAuthenticator", function()
    local authenticator = PasswordAuthenticator("cassandra", "cassandra")
    assert.truthy(authenticator)
  end)
  it("should raise an error if missing a user or password", function()
    assert.has_error(function()
      PasswordAuthenticator()
    end, "no user provided for PasswordAuthenticator")

    assert.has_error(function()
      PasswordAuthenticator("cassandra")
    end, "no password provided for PasswordAuthenticator")
  end)
  it("should authenticate against a cluster with PasswordAuthenticator", function()
    local ok, res, err
    local authenticator = PasswordAuthenticator("cassandra", "cassandra")

    local session = cassandra:new()
    ok, err = session:connect("127.0.0.1", nil, authenticator)
    assert.falsy(err)
    assert.True(ok)

    res, err = session:execute("SELECT * FROM system_auth.users")
    assert.falsy(err)
    assert.truthy(res)
    assert.equal("ROWS", res.type)
  end)
  it("should return an error if no credentials are provided", function()
    local session = cassandra:new()
    local ok, err = session:connect("127.0.0.1")
    assert.False(ok)
    assert.equal("cluster requires authentication, but no authenticator was given to the session", err)
  end)
  it("should return an error if credentials are incorrect", function()
    local authenticator = PasswordAuthenticator("cassandra", "password")

    local session = cassandra:new()
    local ok, err = session:connect("127.0.0.1", nil, authenticator)
    assert.False(ok)
    assert.equal("Cassandra returned error (Bad credentials): Username and/or password are incorrect", err.message)
  end)
end)
