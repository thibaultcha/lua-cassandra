local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

describe("error handling", function()
  local _hosts, _shm

  setup(function()
    _hosts, _shm = utils.ccm_start()
  end)

  describe("spawn_cluster()", function()
    it("should return option errors", function()
      local options = require "cassandra.options"
      spy.on(options, "parse_cluster")
      finally(function()
        options.parse_cluster:revert()
      end)

      local ok, err = cassandra.spawn_cluster()
      assert.False(ok)
      assert.spy(options.parse_cluster).was.called()
      assert.equal("shm is required for spawning a cluster/session", err)

      ok, err = cassandra.spawn_cluster {}
      assert.False(ok)
      assert.equal("shm is required for spawning a cluster/session", err)

      ok, err = cassandra.spawn_cluster {shm = ""}
      assert.False(ok)
      assert.equal("shm must be a valid string", err)
    end)
    it("should return an error when no contact_point is valid", function()
      local contact_points = {"0.0.0.1", "0.0.0.2", "0.0.0.3"}
      local ok, err = cassandra.spawn_cluster {
        shm = "test",
        contact_points = contact_points
      }
      assert.False(ok)
      assert.truthy(string.match(err, "all hosts tried for query failed"))
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
    it("should error when spawning a session without contact_points not cluster", function()
      local shm = "session_without_cluster_nor_contact_points"
      local session, err = cassandra.spawn_session {
        shm = shm
      }
      assert.falsy(session)
      assert.equal("option error: options must contain contact_points to spawn session or cluster", err)
    end)
  end)

  describe("execute()", function()
    local session

    setup(function()
      local err
      session, err = cassandra.spawn_session {
        shm = _shm,
        contact_points = _hosts
      }
      assert.falsy(err)
    end)
    teardown(function()
      session:shutdown()
    end)
    it("should handle CQL errors", function()
      local res, err = session:execute "CAN I HAZ CQL"
      assert.falsy(res)
      assert.equal("[Syntax error] line 1:0 no viable alternative at input 'CAN' ([CAN]...)", err)

      res, err = session:execute "SELECT * FROM system.local WHERE key = ?"
      assert.falsy(res)
      assert.equal("[Invalid] Invalid amount of bind variables", err)
    end)
    it("returns the CQL error code", function()
      local res, err, cql_code = session:execute "CAN I HAZ CQL"
      assert.falsy(res)
      assert.truthy(err)
      assert.equal(cassandra.cql_errors.SYNTAX_ERROR, cql_code)

      res, err, cql_code = session:execute "SELECT * FROM system.local WHERE key = ?"
      assert.falsy(res)
      assert.truthy(err)
      assert.equal(cassandra.cql_errors.INVALID, cql_code)
    end)
  end)

  describe("shm errors", function()
    it("should trigger a cluster refresh if the hosts are not available anymore", function()
      local shm = "test_shm_errors"
      local cache = require "cassandra.cache"
      local dict = cache.get_dict(shm)
      assert.is_table(dict)

      local ok, err = cassandra.spawn_cluster {
        shm = shm,
        contact_points = _hosts
      }
      assert.falsy(err)
      assert.True(ok)
      assert.is_table(cache.get_hosts(shm))

      -- erase hosts from the cache
      dict:delete("hosts")
      assert.falsy(cache.get_hosts(shm))

      -- attempt session create
      local session, err = cassandra.spawn_session {
        shm = shm,
        contact_points = _hosts
      }
      assert.falsy(err)

      -- attempt query
      local rows, err = session:execute "SELECT * FROM system.local"
      assert.falsy(err)
      assert.is_table(rows)
      assert.equal(1, #rows)
    end)
  end)
end)
