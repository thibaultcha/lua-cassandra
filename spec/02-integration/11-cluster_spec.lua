local utils = require "spec.spec_utils"
local Cluster = require "cassandra.cluster"

-- TODO: only to get cql_errors.
-- This will later be require "cassandra"
local host = require "cassandra.host"

describe("cluster", function()
  setup(function()
    utils.ccm_start(3)
  end)

  describe("new()", function()
    it("creates a cluster with default options", function()
      local cluster = assert(Cluster.new())
      assert.same({"127.0.0.1"}, cluster.contact_points)
      assert.is_nil(cluster.keyspace)
    end)
    it("accepts options", function()
      local cluster = assert(Cluster.new {
        contact_points = {"127.0.0.2", "127.0.0.3"},
        keyspace = "system"
      })
      assert.same({"127.0.0.2", "127.0.0.3"}, cluster.contact_points)
      assert.equal("system", cluster.keyspace)
    end)
  end)

  describe("get_first_coordinator()", function()
    it("retrieves the first coordinator to respond", function()
      local cluster = assert(Cluster.new {
        connect_timeout = 100
      })

      local peer = assert(cluster:get_first_coordinator {"127.0.0.255", "127.0.0.1"})
      local rows = assert(peer:execute "SELECT * FROM system.peers")
      assert.equal(2, #rows)

      finally(function()
        peer:close()
      end)
    end)
    it("returns nil when no coordinator replied", function()
      local cluster = assert(Cluster.new {
        connect_timeout = 100
      })

      local peer, err = cluster:get_first_coordinator {"127.0.0.254", "127.0.0.255"}
      assert.equal("all hosts tried for query failed. 127.0.0.254: timeout 127.0.0.255: timeout", err)
      assert.is_nil(peer)
    end)
  end)

  describe("refresh()", function()
    it("refreshes cluster infos in shm", function()
      local cluster = assert(Cluster.new())

      assert(cluster:refresh())

      local cluster_infos = assert(cluster:peers())
      assert.same({"127.0.0.3", "127.0.0.2", "127.0.0.1"}, cluster_infos)

      for _, host in ipairs(cluster_infos) do
        local peer_infos = assert(cluster:get_peer(host))
        assert.same({reconnection_delay = 0, unhealthy_at = 0}, peer_infos)
      end
    end)
    it("complains when no coordinator replied", function()
      local cluster = assert(Cluster.new {
        contact_points = {"127.0.0.254", "127.0.0.255"},
        connect_timeout = 100
      })

      local ok, err = cluster:refresh()
      assert.is_nil(ok)
      assert.equal("all hosts tried for query failed. 127.0.0.254: timeout 127.0.0.255: timeout", err)
    end)
  end)

  describe("get_next_coordinator()", function()
    it("complains if no hosts are in shm", function()
      local cluster = assert(Cluster.new())

      local peer, err = cluster:get_next_coordinator()
      assert.is_nil(peer)
      assert.equal("no hosts to try, must refresh", err)
    end)
    it("retrieves the next healthy peer from the load balancing policy", function()
      -- default is shm round robin policy
      local cluster = assert(Cluster.new())
      assert(cluster:refresh())

      local peer_1 = assert(cluster:get_next_coordinator())
      local rows = assert(peer_1:execute "SELECT * FROM system.peers")
      assert.equal(2, #rows)

      local peer_2 = assert(cluster:get_next_coordinator())
      rows = peer_2:execute "SELECT * FROM system.peers"
      assert.equal(2, #rows)

      assert.not_equal(peer_1.host, peer_2.host)

      finally(function()
        peer_1:close()
        peer_2:close()
      end)
    end)
  end)

  describe("execute()", function()
    it("refreshes automatically if needed", function()
      local cluster = assert(Cluster.new())

      local rows = assert(cluster:execute "SELECT * FROM system.peers")
      assert.equal(2, #rows)
    end)
    it("selects the coordinator from the load balancing policy", function()
      -- default is shm round robin policy
      local cluster = assert(Cluster.new())

      local s = spy.on(cluster, "get_next_coordinator")

      for i = 1, 3 do
        local rows = assert(cluster:execute "SELECT * FROM system.peers")
        assert.equal(2, #rows)
        assert.spy(s).was.called(i)
      end
    end)
    it("spawns hosts in a keyspace", function()
      local cluster = assert(Cluster.new {keyspace = "system"})
      local rows = assert(cluster:execute "SELECT * FROM peers")
      assert.equal(2, #rows)
    end)
    it("prepares and execute at once", function()
      local query = "SELECT * FROM system.peers"
      local cluster = assert(Cluster.new {query_options = {prepared = true}})
      local rows = assert(cluster:execute(query))
      assert.equal(2, #rows)

      local query_id = assert(cluster:get_prepared(query))
      assert.truthy(query_id)
    end)
    it("returns CQL errors", function()
      local cluster = assert(Cluster.new())
      local res, err, code = cluster:execute "SELECT"
      assert.is_nil(res)
      assert.equal("[Syntax error] line 0:-1 no viable alternative at input '<EOF>'", err)
      assert.truthy(code)
      assert.equal(host.cql_errors.SYNTAX_ERROR, code)
    end)
  end)

  describe("shutdown()", function()
    it("flushes all the data in shms", function()
      local cluster = assert(Cluster.new())
      assert(cluster:refresh())

      local keys = cluster.shm:get_keys()
      assert.not_same({}, keys)

      cluster:shutdown()

      keys = cluster.shm:get_keys()
      assert.same({}, keys)
    end)
  end)
end)
