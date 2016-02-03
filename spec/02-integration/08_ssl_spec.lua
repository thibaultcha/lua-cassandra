local cassandra = require "cassandra"
local utils = require "spec.spec_utils"

local SSL_PATH = utils.ssl_path()
local ca_path = SSL_PATH.."/cassandra.pem"
local key_path = SSL_PATH.."/client_key.pem"
local cert_path = SSL_PATH.."/client_cert.pem"

local desc = describe
if _VERSION == "Lua 5.3" then
  -- No SSL spec for Lua 5.3 (LuaSec not compatible yet)
  desc = pending
end

desc("SSL", function()
  local _hosts, _shm

  setup(function()
    _hosts, _shm = utils.ccm_start("ssl", 1, nil, {ssl = true})
  end)

  it("should not connect without SSL", function()
    local ok, err = cassandra.spawn_cluster {
      shm = _shm,
      contact_points = _hosts
    }
    assert.truthy(err)
    assert.equal("NoHostAvailableError", err.type)
    assert.False(ok)
  end)
  it("should connect with SSL without verifying server certificate", function()
    local session, err = cassandra.spawn_session {
      shm = _shm,
      contact_points = _hosts,
      ssl_options = {
        enabled = true
      }
    }
    assert.falsy(err)

    local rows, err = session:execute "SELECT * FROM system.local"
    assert.falsy(err)
    assert.is_table(rows)
    assert.equal(1, #rows)
  end)
  it("should verify server certificate", function()
    local session, err = cassandra.spawn_session {
      shm = _shm,
      contact_points = _hosts,
      ssl_options = {
        enabled = true,
        verify = true,
        ca = ca_path,
      }
    }
    assert.falsy(err)

    local rows, err = session:execute "SELECT * FROM system.local"
    assert.falsy(err)
    assert.is_table(rows)
    assert.equal(1, #rows)
  end)

  describe("client authentication", function()
    setup(function()
      _hosts, _shm = utils.ccm_start("ssl_client_auth", 1, nil, {
        ssl = true,
        require_client_auth = true
      })
    end)

    it("should fail to authentice to server without cert and key", function()
      local session, err = cassandra.spawn_session {
        shm = _shm,
        contact_points = _hosts,
        ssl_options = {
          enabled = true,
          verify = true,
          ca = ca_path
        }
      }
      assert.truthy(err)
      assert.equal("SSLError", err.type)
      assert.falsy(session)
    end)

    it("should authenticate to server", function()
      local session, err = cassandra.spawn_session {
        shm = _shm,
        contact_points = _hosts,
        ssl_options = {
          enabled = true,
          verify = true,
          ca = ca_path,
          key = key_path,
          certificate = cert_path
        }
      }
      assert.falsy(err)

      local rows, err = session:execute "SELECT * FROM system.local"
      assert.falsy(err)
      assert.is_table(rows)
      assert.equal(1, #rows)
    end)
  end)
end)
