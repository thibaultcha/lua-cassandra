local utils = require "spec.spec_utils"
local host = require "cassandra.host"

local SSL_PATH = utils.ssl_path()
local ca_path = SSL_PATH.."/cassandra.pem"
local key_path = SSL_PATH.."/client_key.pem"
local cert_path = SSL_PATH.."/client_cert.pem"

local desc = describe
if _VERSION == "Lua 5.3" then
  -- No SSL spec for Lua 5.3 (LuaSec not compatible yet)
  desc = pending
end

desc("host SSL", function()
  setup(function()
    utils.ccm_start("ssl", 1, nil, {ssl = true})
  end)

  it("does not connect without SSL enabled", function()
    local peer, err = host.new()
    assert.falsy(err)

    local ok, err = peer:connect()
    assert.falsy(ok)
    assert.equal("closed", err)
  end)
  it("connects with SSL", function()
    local peer, err = host.new {ssl = true}
    assert.falsy(err)

    local ok, err = peer:connect()
    assert.falsy(err)
    assert.True(ok)

    local rows, err = peer:execute "SELECT * FROM system.local"
    assert.falsy(err)
    assert.equal(1, #rows)
  end)
  it("connects with SSL and verifying server certificate", function()
    local peer, err = host.new {
      ssl = true,
      verify = true,
      cafile = ca_path
    }
    assert.falsy(err)

    local ok, err = peer:connect()
    assert.falsy(err)
    assert.True(ok)

    local rows, err = peer:execute "SELECT * FROM system.local"
    assert.falsy(err)
    assert.equal(1, #rows)
  end)
end)
