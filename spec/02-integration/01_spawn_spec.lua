local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

describe("", function()
  local _hosts, _shm

  setup(function()
    _hosts, _shm = utils.ccm_start()
  end)

  describe("spawn_cluster()", function()
    it("should spawn a cluster", function()
      local ok, err = cassandra.spawn_cluster {
        shm = _shm,
        contact_points = _hosts
      }
      assert.falsy(err)
      assert.True(ok)
    end)
    it("should retrieve cluster infos in spawned cluster's shm", function()
      local cache = require "cassandra.cache"
      local hosts, err = cache.get_hosts(_shm)
      assert.falsy(err)
      -- index of hosts
      assert.equal(#_hosts, #hosts)
      -- hosts details
      for _, host_addr in ipairs(hosts) do
        local host_details = cache.get_host(_shm, host_addr)
        assert.truthy(host_details)
      end
    end)
    it("should iterate over contact_points to find an entrance into the cluster", function()
      local contact_points = {"0.0.0.1", "0.0.0.2", "0.0.0.3"}
      contact_points[#contact_points + 1] = _hosts[1]

      local ok, err = cassandra.spawn_cluster({
        shm = "test",
        contact_points = contact_points
      })
      assert.falsy(err)
      assert.True(ok)
    end)
    it("should accept a custom port for given hosts", function()
      local contact_points = {}
      for i, addr in ipairs(_hosts) do
        contact_points[i] = addr..":9043"
      end
      local ok, err = cassandra.spawn_cluster({
        shm = "test",
        contact_points = contact_points
      })
      assert.truthy(err)
      assert.False(ok)
      assert.equal("NoHostAvailableError", err.type)
    end)
    it("should accept a custom port through an option", function()
      local ok, err = cassandra.spawn_cluster({
        shm = "test",
        protocol_options = {default_port = 9043},
        contact_points = _hosts
      })
      assert.truthy(err)
      assert.False(ok)
      assert.equal("NoHostAvailableError", err.type)
    end)
  end)

  describe("spawn_session()", function()
    it("should spawn a session", function()
      local session, err = cassandra.spawn_session {shm = _shm}
      assert.falsy(err)
      assert.truthy(session)
      assert.truthy(session.hosts)
      assert.equal(#_hosts, #session.hosts)
    end)
    it("should spawn a session without having to spawn a cluster", function()
      local shm = "session_without_cluster"
      local t_session, err = cassandra.spawn_session {
        shm = shm,
        contact_points = _hosts
      }
      assert.falsy(err)
      assert.truthy(t_session)
      -- Check cache
      local cache = require "cassandra.cache"
      local hosts, err = cache.get_hosts(shm)
      assert.falsy(err)
      -- index of hosts
      assert.equal(#_hosts, #hosts)
      -- hosts details
      for _, host_addr in ipairs(hosts) do
        local host_details = cache.get_host(shm, host_addr)
        assert.truthy(host_details)
      end
    end)
    it("should spawn a session in a given keyspace", function()
      local session_in_keyspace, err = cassandra.spawn_session({
        shm = _shm,
        keyspace = "system"
      })
      assert.falsy(err)
      assert.equal("system", session_in_keyspace.options.keyspace)
      assert.equal("system", session_in_keyspace.hosts[1].options.keyspace)

      local rows, err = session_in_keyspace:execute "SELECT * FROM local"
      assert.falsy(err)
      assert.is_table(rows)
      assert.equal(1, #rows)
    end)
  end)
end)
