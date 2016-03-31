local load_balancing_policies = require "cassandra.policies.load_balancing"

describe("Load balancing policies", function()
  describe("shared round robin", function()
    local shared_round_robin = load_balancing_policies.shared_round_robin
    local hosts = {"127.0.0.1", "127.0.0.2", "127.0.0.3"}
    local shm

    before_each(function()
      local shared = require "cassandra.utils.shm"
      shm = shared.new()
    end)

    it("should iterate over the hosts in a round robin fashion", function()
      local iter = shared_round_robin(shm, hosts)
      assert.equal("127.0.0.1", select(2, iter()))
      assert.equal("127.0.0.2", select(2, iter()))
      assert.equal("127.0.0.3", select(2, iter()))
    end)
    it("should start at a different indexes for each iterator", function()
      local iter1 = shared_round_robin(shm, hosts)
      local iter2 = shared_round_robin(shm, hosts)
      local iter3 = shared_round_robin(shm, hosts)

      assert.equal("127.0.0.1", select(2, iter1())) -- iter 1 starts on index 1
      assert.equal("127.0.0.2", select(2, iter2())) -- iter 2 starts on index 2
      assert.equal("127.0.0.3", select(2, iter3())) -- iter 3 starts on index 3

      assert.equal("127.0.0.2", select(2, iter1()))
      assert.equal("127.0.0.3", select(2, iter1()))

      assert.equal("127.0.0.3", select(2, iter2()))
      assert.equal("127.0.0.1", select(2, iter3()))
      assert.equal("127.0.0.1", select(2, iter2()))
      assert.equal("127.0.0.2", select(2, iter3()))
    end)
    it("should be callable in a loop", function()
      assert.has_no_errors(function()
        local i = 0
        for _, host in shared_round_robin(shm, hosts) do
          i = i + 1
        end
        assert.equal(3, i)
      end)
    end)
  end)
end)
