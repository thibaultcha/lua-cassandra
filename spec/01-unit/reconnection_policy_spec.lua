local reconnection_policies = require "cassandra.policies.reconnection"

local host_options = {
  shm = "cassandra",
  protocol_options = {
    default_port = 9042
  }
}

describe("Reconnection policies", function()
  describe("constant", function()
    local delay = 5000
    local policy = reconnection_policies.Constant(delay)

    it("should return a constant delay", function()
      assert.has_no_error(function()
        policy.new_schedule()
      end)

      for i = 1, 10 do
        assert.equal(delay, policy.next())
      end
    end)
  end)
  describe("exponential", function()
    local base_delay, max_delay = 1000, 10 * 60 * 1000
    local policy = reconnection_policies.SharedExponential(base_delay, max_delay)
    local host1 = {address = "127.0.0.1", options = host_options}
    local host2 = {address = "127.0.0.2", options = host_options}

    policy.new_schedule(host1)
    policy.new_schedule(host2)

    it("should return an exponential delay", function()
      assert.equal(1000, policy.next(host1))
      assert.equal(4000, policy.next(host1))
      assert.equal(9000, policy.next(host1))
      assert.equal(16000, policy.next(host1))
      assert.equal(25000, policy.next(host1))
      for i = 1, 19 do
        policy.next(host1)
      end
      assert.equal(600000, policy.next(host1))
      assert.equal(600000, policy.next(host1))
    end)
    it("should allow for different schedules", function()
      assert.equal(1000, policy.next(host2))
      assert.equal(4000, policy.next(host2))
      assert.equal(9000, policy.next(host2))
      assert.equal(16000, policy.next(host2))
    end)
    it("should be possible to schedule a new policy, aka 'reset' it", function()
      policy.new_schedule(host1)
      assert.equal(1000, policy.next(host1))
      assert.equal(4000, policy.next(host1))
    end)
  end)
end)
