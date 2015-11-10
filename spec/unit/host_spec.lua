local Host = require "cassandra.host"

local opts = {
  logger = {
    warn = function()end,
    info = function()end
  }
}

describe("Host", function()
  local host
  it("should be instanciable", function()
    host = Host("127.0.0.1:9042", opts)
    host.reconnection_delay = 0
    assert.equal(0, host.unhealthy_at)
    assert.True(host:can_be_considered_up())
  end)
  it("should be possible to mark it as DOWN", function()
    host:set_down()
    assert.equal(os.time() * 1000, host.unhealthy_at)
    assert.False(host:can_be_considered_up())
  end)
  it("should be possible to mark as UP", function()
    host:set_up()
    assert.equal(0, host.unhealthy_at)
    assert.True(host:can_be_considered_up())
  end)
end)
