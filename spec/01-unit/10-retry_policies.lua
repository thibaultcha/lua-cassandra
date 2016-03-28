local retry_policies = require "cassandra.policies.retry"

describe("retry policies", function()
  describe("simple", function()
    local simple_retry = retry_policies.simple.new(3)

    it("retries n times on read/write timeouts", function()
      assert.True(simple_retry:on_read_timeout {n_retries = 1})
      assert.False(simple_retry:on_read_timeout {n_retries = 3})
      assert.True(simple_retry:on_write_timeout {n_retries = 1})
      assert.False(simple_retry:on_write_timeout {n_retries = 3})
    end)
    it("does not retry on unavailable", function()
      assert.False(simple_retry:on_unavailable())
    end)
  end)
end)
