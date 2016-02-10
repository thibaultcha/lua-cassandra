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

  describe("luasocket fallback", function()
    it("should fallback on proxy socket", function()
      local socket = session.hosts[1].socket
      assert.True(socket.fallback)
    end)
    it("should proxy settimeout()", function()
      local socket = session.hosts[1].socket
      local mt = getmetatable(socket)
      assert.truthy(mt)

      spy.on(mt, "settimeout")

      local session_t = cassandra.spawn_session {
        shm = _shm,
        socket_options = {
          connect_timeout = 1000,
          read_timeout = 1000
        }
      }

      -- force connect
      session_t:execute "SELECT * FROM system.local"

      assert.spy(mt.settimeout).was.called(2) -- connect + read timeout
    end)
    it("should proxy getreusedtimes()", function()
      local socket = session.hosts[1].socket
      assert.equal(0, socket:getreusedtimes())
    end)
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
      assert.is_table(err)
      assert.equal("NoHostAvailableError", err.type)
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
