local policies = require "cassandra.policies.reconnection"

describe("reconnection policies", function()
  describe("constant", function()
    local policy = policies.constant(5000)

    it("returns a constant delay", function()
      for i = 1, 10 do
        assert.equal(5000, policy.get_next())
      end
    end)
  end)
  describe("shared_exp", function()
    local shm = require "cassandra.utils.shm"
    local base_delay, max_delay = 1000, 10 * 60 * 1000
    local policy = policies.shared_exp(shm.new(), base_delay, max_delay)

    local coordinator1 = "127.0.0.1"
    local coordinator2 = "127.0.0.2"

    it("returns an exponential delay", function()
      assert.equal(1000, policy.get_next(coordinator1))
      assert.equal(4000, policy.get_next(coordinator1))
      assert.equal(9000, policy.get_next(coordinator1))
      assert.equal(16000, policy.get_next(coordinator1))
      assert.equal(25000, policy.get_next(coordinator1))
      for i = 1, 19 do
        policy.get_next(coordinator1)
      end
      assert.equal(600000, policy.get_next(coordinator1))
      assert.equal(600000, policy.get_next(coordinator1))
    end)
    it("allows for different schedules at the same time", function()
      assert.equal(1000, policy.get_next(coordinator2))
      assert.equal(4000, policy.get_next(coordinator2))
      assert.equal(9000, policy.get_next(coordinator2))
      assert.equal(16000, policy.get_next(coordinator2))
    end)
    it("resets", function()
      policy.reset(coordinator1)
      assert.equal(1000, policy.get_next(coordinator1))
      assert.equal(4000, policy.get_next(coordinator1))
    end)
  end)
end)
