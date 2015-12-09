local cassandra = require "cassandra"

describe("options parsing", function()
  describe("spawn_cluster", function()
    it("should require shm", function()
      local err = select(2, cassandra.spawn_cluster())
      assert.equal("shm is required for spawning a cluster/session", err)

      err = select(2, cassandra.spawn_cluster({shm = 123}))
      assert.equal("shm must be a string", err)

      err = select(2, cassandra.spawn_cluster({shm = ""}))
      assert.equal("shm must be a valid string", err)
    end)
    it("should require contact_points", function()
      local err = select(2, cassandra.spawn_cluster({shm = "test"}))
      assert.equal("contact_points option is required", err)

      err = select(2, cassandra.spawn_cluster({shm = "test", contact_points = {}}))
      assert.equal("contact_points must contain at least one contact point", err)

      err = select(2, cassandra.spawn_cluster({shm = "test", contact_points = {foo = "bar"}}))
      assert.equal("contact_points must be an array (integer-indexed table)", err)
    end)
  end)
  describe("spawn_session", function()
    it("should require shm", function()
      local err = select(2, cassandra.spawn_session())
      assert.equal("shm is required for spawning a cluster/session", err)

      err = select(2, cassandra.spawn_session({shm = 123}))
      assert.equal("shm must be a string", err)

      err = select(2, cassandra.spawn_session({shm = ""}))
      assert.equal("shm must be a valid string", err)
    end)
    it("should validate protocol_options", function()
      local err = select(2, cassandra.spawn_session({
        shm = "test",
        protocol_options = {
          default_port = ""
        }
      }))

      assert.equal("protocol default_port must be a number", err)
    end)
    it("should validate policies", function()
      local err = select(2, cassandra.spawn_session({
        shm = "test",
        policies = {
          address_resolution = ""
        }
      }))

      assert.equal("address_resolution policy must be a function", err)
    end)
  end)
end)
