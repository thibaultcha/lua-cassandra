local utils = require "spec.spec_utils"
local Cluster = require "cassandra.cluster"

describe("cluster", function()
  local hosts
  setup(function()
    hosts = utils.ccm_start("cluster", 3)
  end)

  describe("new()", function()
    it("creates a cluster with default options", function()
      local cluster, err = Cluster.new()
      assert.falsy(err)
      assert.truthy(cluster)
      assert.same({"127.0.0.1"}, cluster.contact_points)
      assert.falsy(cluster.keyspace)
    end)
    it("accepts options", function()
      local cluster, err = Cluster.new {
        contact_points = {"127.0.0.2", "127.0.0.3"},
        keyspace = "system"
      }
      assert.falsy(err)
      assert.same({"127.0.0.2", "127.0.0.3"}, cluster.contact_points)
      assert.equal("system", cluster.keyspace)
    end)
  end)

  describe("get_first_coordinator()", function()
    it("retrieves the first coordinator to respond", function()
      local cluster, err = Cluster.new()
      assert.falsy(err)

      local peer, err = cluster:get_first_coordinator {"127.0.0.255", "127.0.0.1"}
      assert.falsy(err)
      assert.truthy(peer)

      local rows, err = peer:execute "SELECT * FROM system.peers"
      assert.falsy(err)
      assert.equal(2, #rows)

      finally(function()
        peer:close()
      end)
    end)
    it("returns nil when no coordinator replied", function()
      local cluster, err = Cluster.new()
      assert.falsy(err)

      local peer, err = cluster:get_first_coordinator {"127.0.0.254", "127.0.0.255"}
      assert.falsy(peer)
      assert.equal("all hosts tried for query failed. 127.0.0.254: timeout 127.0.0.255: timeout", err)
    end)
  end)

  describe("refresh()", function()
    it("refreshes cluster infos in shm", function()
      local cluster, err = Cluster.new()
      assert.falsy(err)

      local ok, err = cluster:refresh()
      assert.falsy(err)
      assert.True(ok)

      local shm = cluster.shm
      local cluster_infos, err = cluster:peers()
      assert.falsy(err)
      assert.same({"127.0.0.3", "127.0.0.2", "127.0.0.1"}, cluster_infos)

      for _, host in ipairs(cluster_infos) do
        local peer_infos, err = cluster:get_peer(host)
        assert.falsy(err)
        assert.same({reconnection_delay = 0, unhealthy_at = 0}, peer_infos)
      end
    end)
    it("complains when no coordinator replied", function()
      local cluster, err = Cluster.new {
        contact_points = {"127.0.0.254", "127.0.0.255"}
      }
      assert.falsy(err)

      local ok, err = cluster:refresh()
      assert.falsy(ok)
      assert.equal("all hosts tried for query failed. 127.0.0.254: timeout 127.0.0.255: timeout", err)
    end)
  end)

  describe("get_next_coordinator()", function()
    it("complains if no hosts are in shm", function()
      local cluster, err = Cluster.new()
      assert.falsy(err)

      local peer, err = cluster:get_next_coordinator()
      assert.falsy(peer)
      assert.equal("no hosts to try, must refresh", err)
    end)
    it("retrieves the next healthy peer from the load balancing policy", function()
      -- default is shm round robin policy
      local cluster, err = Cluster.new()
      assert.falsy(err)

      local _, err = cluster:refresh()
      assert.falsy(err)

      local peer_1, err = cluster:get_next_coordinator()
      assert.falsy(err)
      assert.truthy(peer_1)

      local rows, err = peer_1:execute "SELECT * FROM system.peers"
      assert.falsy(err)
      assert.equal(2, #rows)

      local peer_2, err = cluster:get_next_coordinator()
      assert.falsy(err)
      assert.truthy(peer_2)

      rows, err = peer_2:execute "SELECT * FROM system.peers"
      assert.falsy(err)
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
      local cluster, err = Cluster.new()
      assert.falsy(err)

      local rows, err = cluster:execute "SELECT * FROM system.peers"
      assert.falsy(err)
      assert.equal(2, #rows)
    end)
    it("selects the coordinator from the load balancing policy", function()
      -- default is shm round robin policy
      local cluster, err = Cluster.new()
      assert.falsy(err)

      local s = spy.on(cluster, "get_next_coordinator")

      local _, err = cluster:execute "SELECT * FROM system.peers"
      assert.falsy(err)

      assert.spy(s).was.called()
    end)
  end)

  describe("shutdown()", function()
    it("flushes all the data in shms", function()
      local cluster, err = Cluster.new()
      assert.falsy(err)

      local _, err = cluster:refresh()
      assert.falsy(err)

      local keys = cluster.shm:get_keys()
      assert.not_same({}, keys)

      cluster:shutdown()

      keys = cluster.shm:get_keys()
      assert.same({}, keys)
    end)
  end)
end)
