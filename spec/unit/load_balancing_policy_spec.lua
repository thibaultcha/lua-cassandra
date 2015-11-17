local load_balancing_policies = require "cassandra.policies.load_balancing"

describe("Load balancing policies", function()
  describe("Shared round robin", function()
    local SharedRoundRobin = load_balancing_policies.SharedRoundRobin
    local shm = "cassandra"
    local hosts = {"127.0.0.1", "127.0.0.2", "127.0.0.3"}

    it("should iterate over the hosts in a round robin fashion", function()
      local iter = SharedRoundRobin(shm, hosts)
      assert.equal("127.0.0.1", select(2, iter()))
      assert.equal("127.0.0.2", select(2, iter()))
      assert.equal("127.0.0.3", select(2, iter()))
    end)
    it("should share its state accros different iterators", function()
      local iter1 = SharedRoundRobin(shm, hosts)
      local iter2 = SharedRoundRobin(shm, hosts)
      assert.equal("127.0.0.1", select(2, iter1()))
      assert.equal("127.0.0.2", select(2, iter2()))
      assert.equal("127.0.0.3", select(2, iter1()))
    end)
    it("should be callable in a loop", function()
      assert.has_no_errors(function()
        local i = 0
        for _, host in SharedRoundRobin(shm, hosts) do
          i = i + 1
        end
        assert.equal(3, i)
      end)
    end)
  end)
end)
