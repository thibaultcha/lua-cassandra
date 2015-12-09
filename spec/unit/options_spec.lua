local options = require "cassandra.options"
local parse_cluster = options.parse_cluster
local parse_session = options.parse_session

describe("options parsing", function()
  describe("parse_cluster", function()
    it("should require shm", function()
      local err = select(2, parse_cluster())
      assert.equal("shm is required for spawning a cluster/session", err)

      err = select(2, parse_cluster({shm = 123}))
      assert.equal("shm must be a string", err)

      err = select(2, parse_cluster({shm = ""}))
      assert.equal("shm must be a valid string", err)
    end)
    it("should require contact_points", function()
      local err = select(2, parse_cluster({shm = "test"}))
      assert.equal("contact_points option is required", err)

      err = select(2, parse_cluster({shm = "test", contact_points = {}}))
      assert.equal("contact_points must contain at least one contact point", err)

      err = select(2, parse_cluster({shm = "test", contact_points = {foo = "bar"}}))
      assert.equal("contact_points must be an array (integer-indexed table)", err)
    end)
    it("should ignore `keyspace` if given", function()
      local options, err = parse_cluster {
        shm = "test",
        contact_points = {"127.0.0.1"},
        keyspace = "foo"
      }
      assert.falsy(err)
      assert.falsy(options.keyspace)
    end)
  end)
  describe("parse_session", function()
    it("should require shm", function()
      local err = select(2, parse_session())
      assert.equal("shm is required for spawning a cluster/session", err)

      err = select(2, parse_session({shm = 123}))
      assert.equal("shm must be a string", err)

      err = select(2, parse_session({shm = ""}))
      assert.equal("shm must be a valid string", err)
    end)
    it("should validate keyspace if given", function()
      local err = select(2, parse_session({shm = "test", keyspace = 123}))
      assert.equal("keyspace must be a valid string", err)

      err = select(2, parse_session({shm = "test", keyspace = ""}))
      assert.equal("keyspace must be a valid string", err)
    end)
    it("should validate protocol_options", function()
      local err = select(2, parse_session({
        shm = "test",
        protocol_options = {
          default_port = ""
        }
      }))
      assert.equal("protocol default_port must be a number", err)

      err = select(2, parse_session({
        shm = "test",
        protocol_options = {
          max_schema_consensus_wait = ""
        }
      }))
      assert.equal("protocol max_schema_consensus_wait must be a number", err)
    end)
    it("should validate policies", function()
      local err = select(2, parse_session({
        shm = "test",
        policies = {
          address_resolution = ""
        }
      }))
      assert.equal("address_resolution policy must be a function", err)

      -- @TODO
      -- validate other policies (need to freeze the API)
    end)
    it("should validate query options", function()
      local err = select(2, parse_session({
        shm = "test",
        query_options = {
          page_size = ""
        }
      }))
      assert.equal("query page_size must be a number", err)
    end)
    it("should validate socket options", function()
      local err = select(2, parse_session({
        shm = "test",
        socket_options = {
          connect_timeout = ""
        }
      }))
      assert.equal("socket connect_timeout must be a number", err)

      err = select(2, parse_session({
        shm = "test",
        socket_options = {
          read_timeout = ""
        }
      }))
      assert.equal("socket read_timeout must be a number", err)
    end)
    it("should set `prepared_shm` to `shm` if nil", function()
      local options, err = parse_session {
        shm = "test"
      }
      assert.falsy(err)
      assert.equal("test", options.prepared_shm)

      options, err = parse_session {
        shm = "test",
        prepared_shm = "prepared_test"
      }
      assert.falsy(err)
      assert.equal("prepared_test", options.prepared_shm)
    end)
  end)
end)
