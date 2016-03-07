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
  end)
end)
