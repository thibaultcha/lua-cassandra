local cassandra = require "cassandra"
local utils = require "spec.spec_utils"

local SSL_ENABLED = false -- disabled while LuaSec doesn't support Lua 5.3 (we still want auth tests)
local SSL_PATH = utils.ssl_path()
local ca_path = SSL_PATH.."/cassandra.pem"

describe("PasswordAuthenticator", function()
  local _hosts, _shm

  setup(function()
    _hosts, _shm = utils.ccm_start("auth", 1, nil, {
      ssl = SSL_ENABLED,
      pwd_auth = true
    })
  end)

  it("should complain if not auth provider was configured", function()
    local session, err = cassandra.spawn_session {
      shm = _shm,
      contact_points = _hosts,
      ssl_options = {
        enabled = SSL_ENABLED,
        verify = true,
        ca = ca_path
      }
    }
    assert.equal("Host at 127.0.0.1:9042 required authentication but no auth provider was configured for session", err)
    assert.falsy(session)
  end)
  it("should be refused if credentials are invalid", function()
    local session, err = cassandra.spawn_session {
      shm = _shm,
      contact_points = _hosts,
      ssl_options = {
        enabled = SSL_ENABLED,
        verify = true,
        ca = ca_path
      },
      auth = cassandra.auth.PlainTextProvider("cassandra", "invalid")
    }
    assert.equal("[Bad credentials] Username and/or password are incorrect", err)
    assert.falsy(session)
  end)
  it("should authenticate with valid credentials", function()
    local session, err = cassandra.spawn_session {
      shm = _shm,
      contact_points = _hosts,
      ssl_options = {
        enabled = SSL_ENABLED,
        verify = true,
        ca = ca_path
      },
      auth = cassandra.auth.PlainTextProvider("cassandra", "cassandra")
    }
    assert.falsy(err)

    local rows, err = session:execute "SELECT * FROM system.local"
    assert.falsy(err)
    assert.is_table(rows)
    assert.equal(1, #rows)
  end)
end)
