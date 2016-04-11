local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

describe("session", function()
  local session, _hosts, _shm

  setup(function()
    _hosts, _shm = utils.ccm_start()
  end)

  before_each(function()
    local err
    session, err = cassandra.spawn_session {
      shm = _shm,
      contact_points = _hosts
    }
    assert.falsy(err)

    -- force connect
    session:execute "SELECT * FROM system.local"
  end)

  describe("set_keyspace()", function()
    it("should set a session's 'keyspace' option", function()
      local ok, err = session:set_keyspace "system"
      assert.falsy(err)
      assert.True(ok)
      assert.equal("system", session.options.keyspace)

      local rows, err = session:execute "SELECT * FROM local"
      assert.falsy(err)
      assert.is_table(rows)
      assert.equal(1, #rows)
    end)
  end)

  describe("shutdown()", function()
    it("should close all connections and make the session unusable", function()
      session:shutdown()
      assert.True(session.terminated)
      assert.same({}, session.hosts)

      local rows, err = session:execute "SELECT * FROM system.local"
      assert.equal("cannot reuse a session that has been shut down", err)
      assert.falsy(rows)
    end)
  end)

  describe("set_keep_alive()", function()
    it("should fallback to shutdown() when outside of ngx_lua", function()
      local rows, err = session:execute "SELECT * FROM system.local"
      assert.falsy(err)
      assert.equal(1, #rows)

      assert.has_no_error(function()
        session:set_keep_alive()
      end)

      -- However, it does not terminate the session
      rows, err = session:execute "SELECT * FROM system.local"
      assert.falsy(err)
      assert.is_table(rows)
      assert.equal(1, #rows)
    end)
  end)
end)
