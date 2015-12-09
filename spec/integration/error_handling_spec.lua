local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

local LOG_LVL = "ERR"

-- Define log level for tests
utils.set_log_lvl(LOG_LVL)

local _shm = "cassandra_error_specs"
local _hosts = utils.hosts

describe("error handling", function()
  describe("spawn_cluster()", function()
    it("should return option errors", function()
      local options = require "cassandra.options"
      spy.on(options, "parse_cluster")
      finally(function()
        options.parse_cluster:revert()
      end)

      local cluster, err = cassandra.spawn_cluster()
      assert.falsy(cluster)
      assert.spy(options.parse_cluster).was.called()
      assert.equal("shm is required for spawning a cluster/session", err)

      cluster, err = cassandra.spawn_cluster {}
      assert.falsy(cluster)
      assert.equal("shm is required for spawning a cluster/session", err)

      cluster, err = cassandra.spawn_cluster {shm = ""}
      assert.falsy(cluster)
      assert.equal("shm must be a valid string", err)
    end)
    it("should return an error when no contact_point is valid", function()
      utils.set_log_lvl("QUIET")
      finally(function()
        utils.set_log_lvl(LOG_LVL)
      end)

      local contact_points = {"0.0.0.1", "0.0.0.2", "0.0.0.3"}
      local cluster, err = cassandra.spawn_cluster {
        shm = "test",
        contact_points = contact_points
      }
      assert.truthy(err)
      assert.falsy(cluster)
      assert.equal("NoHostAvailableError", err.type)
    end)
  end)
  describe("shorthand serializers", function()
    it("should require the first argument (value)", function()
      assert.has_error(cassandra.uuid, "argument #1 required for 'uuid' type shorthand")
      assert.has_error(cassandra.map, "argument #1 required for 'map' type shorthand")
      assert.has_error(cassandra.list, "argument #1 required for 'list' type shorthand")
      assert.has_error(cassandra.timestamp, "argument #1 required for 'timestamp' type shorthand")
      local trace = debug.traceback()
      local match = string.find(trace, "stack traceback:\n\tspec/integration/error_handling_spec.lua", nil, true)
      assert.equal(1, match)
    end)
  end)
  describe("spawn_session()", function()
    it("should return options errors", function()
      local options = require "cassandra.options"
      spy.on(options, "parse_session")
      finally(function()
        options.parse_session:revert()
      end)

      local session, err = cassandra.spawn_session()
      assert.falsy(session)
      assert.spy(options.parse_session).was.called()
      assert.equal("shm is required for spawning a cluster/session", err)
    end)
  end)
  describe("execute()", function()
    local session

    setup(function()
      local cluster, err = cassandra.spawn_cluster {
        shm = _shm,
        contact_points = _hosts
      }
      assert.falsy(err)

      session, err = cluster:spawn_session {shm = _shm}
      assert.falsy(err)
    end)
    teardown(function()
      session:shutdown()
    end)
    it("should handle CQL errors", function()
      local res, err = session:execute("CAN I HAZ CQL")
      assert.falsy(res)
      assert.truthy(err)
      assert.equal("ResponseError", err.type)

      res, err = session:execute("SELECT * FROM system.local WHERE key = ?")
      assert.falsy(res)
      assert.truthy(err)
      assert.equal("ResponseError", err.type)
    end)
  end)
  describe("shm errors", function()
    it("should trigger a cluster refresh if the hosts are not available anymore", function()
      local shm = "test_shm_errors"
      local cache = require "cassandra.cache"
      local dict = cache.get_dict(shm)
      assert.truthy(dict)

      local cluster, err = cassandra.spawn_cluster {
        shm = shm,
        contact_points = _hosts
      }
      assert.falsy(err)
      assert.truthy(cache.get_hosts(shm))

      -- erase hosts from the cache
      dict:delete("hosts")
      assert.falsy(cache.get_hosts(shm))

      -- attempt session create
      local session, err = cluster:spawn_session()
      assert.falsy(err)

      -- attempt query
      local rows, err = session:execute("SELECT * FROM system.local")
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(1, #rows)
    end)
  end)
end)
