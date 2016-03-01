local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

describe("session", function()
  local _hosts, _shm
  setup(function()
    _hosts, _shm = utils.ccm_start()
  end)

  describe("new()", function()
    it("spawns a session", function()
      local session, err = cassandra.new {
        shm = _shm,
        contact_points = _hosts
      }
      assert.falsy(err)
      assert.truthy(session)
    end)
    it("stores cluster infos in shm", function()
      local _, err = cassandra.new {
        shm = "empty_shm",
        contact_points = _hosts
      }
      assert.falsy(err)

      local cache = require "cassandra.cache"
      local hosts, err = cache.get_hosts "empty_shm"
      assert.falsy(err)
      assert.is_table(hosts)
      assert.equal(#_hosts, #hosts)
      for _, host_addr in ipairs(hosts) do
        local host_details, err = cache.get_host(_shm, host_addr)
        assert.falsy(err)
        assert.truthy(host_details)
      end
    end)
    it("spawns a session in given keyspace", function()
      local session_in_keyspace, err = cassandra.new {
        shm = _shm,
        contact_points = _hosts,
        keyspace = "system"
      }
      assert.falsy(err)
      assert.equal("system", session_in_keyspace.options.keyspace)
      assert.equal("system", session_in_keyspace.hosts[1].options.keyspace)

      local rows, err = session_in_keyspace:execute "SELECT * FROM local"
      assert.falsy(err)
      assert.is_table(rows)
      assert.equal(1, #rows)
    end)
    it("iterates over contact_points to find an entrance into the cluster", function()
      local contact_points = {"0.0.0.1", "0.0.0.2", "0.0.0.3"}
      contact_points[#contact_points + 1] = _hosts[1]

      local session, err = cassandra.new {
        shm = "test",
        contact_points = contact_points
      }
      assert.falsy(err)
      assert.truthy(session)
    end)
    it("accepts a custom port for given hosts", function()
      local contact_points = {}
      for i, addr in ipairs(_hosts) do
        contact_points[i] = addr..":9043"
      end
      local session, err = cassandra.new {
        shm = "test_2",
        contact_points = contact_points
      }
      assert.truthy(err)
      assert.falsy(session)
      assert.equal("all hosts tried for query failed. 127.0.0.1:9043: connection refused.", err)
    end)
    it("accepts a custom port through an option", function()
      local session, err = cassandra.new {
        shm = "test_3",
        contact_points = _hosts,
        protocol_options = {
          default_port = 9043
        }
      }
      assert.truthy(err)
      assert.falsy(session)
      assert.equal("all hosts tried for query failed. 127.0.0.1:9043: connection refused.", err)
    end)

    describe("errors", function()
      it("refuses invalid options", function()
        local session, err = cassandra.new()
        assert.falsy(session)
        assert.equal("shm is required", err)

        session, err = cassandra.new {shm = _shm}
        assert.falsy(session)
        assert.equal("contact_points is required", err)
      end)
    end)
  end)

  describe(function()
    local session

    before_each(function()
      local err
      session, err = cassandra.new {
        shm = _shm,
        contact_points = _hosts
      }
      assert.falsy(err)
    end)

    describe("set_keyspace()", function()
      it("changes a session keyspace", function()
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
      it("closes all connections and make the session unusable", function()
        session:shutdown()
        assert.True(session.terminated)
        assert.same({}, session.hosts)

        local rows, err = session:execute "SELECT * FROM system.local"
        assert.equal("cannot reuse a session that has been shut down", err)
        assert.falsy(rows)
      end)
    end)

    describe("set_keep_alive()", function()
      it("fallbacks to shutdown() when outside of ngx_lua", function()
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
end)
