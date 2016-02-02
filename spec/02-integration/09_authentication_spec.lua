local cassandra = require "cassandra"
local utils = require "spec.spec_utils"

local SSL_PATH = utils.ssl_path()
local ca_path = SSL_PATH.."/cassandra.pem"

describe("PasswordAuthenticator", function()
  local _hosts, _shm

  setup(function()
    _hosts, _shm = utils.ccm_start("auth", 1, nil, {
      ssl = false,
      pwd_auth = true
    })
  end)

  it("should be refused if credentials are invalid", function()
    local session, err = cassandra.spawn_session {
      shm = _shm,
      contact_points = _hosts,
      ssl_options = {
        enabled = false,
        verify = true,
        ca = ca_path
      },
      username = "cassandra",
      password = "wrong"
    }
    assert.truthy(err)
    assert.equal("AuthenticationError", err.type)
    assert.falsy(session)
  end)
  it("should authenticate with valid credentials", function()
    local session, err = cassandra.spawn_session {
      shm = _shm,
      contact_points = _hosts,
      ssl_options = {
        enabled = false,
        verify = true,
        ca = ca_path
      },
      username = "cassandra",
      password = "cassandra"
    }
    assert.falsy(err)

    local rows, err = session:execute "SELECT * FROM system.local"
    assert.falsy(err)
    assert.is_table(rows)
    assert.equal(1, #rows)
  end)
end)
