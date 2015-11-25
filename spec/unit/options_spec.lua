local cassandra = require "cassandra"

describe("options parsing", function()
  describe("spawn_cluster", function()
    it("should require shm", function()
      assert.has_error(function()
        cassandra.spawn_cluster()
      end, "shm is required for spawning a cluster/session")

      assert.has_error(function()
        cassandra.spawn_cluster({shm = 123})
      end, "shm must be a string")

      assert.has_error(function()
        cassandra.spawn_cluster({shm = ""})
      end, "shm must be a valid string")
    end)
    it("should require contact_points", function()
      assert.has_error(function()
        cassandra.spawn_cluster({
          shm = "test"
        })
      end, "contact_points option is required")

      assert.has_error(function()
        cassandra.spawn_cluster({
          shm = "test",
          contact_points = {}
        })
      end, "contact_points must contain at least one contact point")

      assert.has_error(function()
        cassandra.spawn_cluster({
          shm = "test",
          contact_points = {foo = "bar"}
        })
      end, "contact_points must be an array (integer-indexed table)")
    end)
  end)
  describe("spawn_session", function()
    it("should require shm", function()
      assert.has_error(function()
        cassandra.spawn_session()
      end, "shm is required for spawning a cluster/session")

      assert.has_error(function()
        cassandra.spawn_session({shm = 123})
      end, "shm must be a string")

      assert.has_error(function()
        cassandra.spawn_session({shm = ""})
      end, "shm must be a valid string")
    end)
    it("should validate protocol_options", function()
      assert.has_error(function()
        cassandra.spawn_session({
          shm = "test",
          protocol_options = {
            default_port = ""
          }
        })
      end, "protocol default_port must be a number")
    end)
    it("should validate policies", function()
      assert.has_error(function()
        cassandra.spawn_session({
          shm = "test",
          policies = {
            address_resolution = ""
          }
        })
      end, "address_resolution policy must be a function")
    end)
  end)
end)
