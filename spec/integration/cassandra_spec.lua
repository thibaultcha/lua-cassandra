--- Pure Lua integration tests.
-- lua-cassandra is built with support for pure Lua, outside of ngx_lua,
-- with fallback on LuaSocket when it is the case. Those integration tests must
-- mimic the ones running in ngx_lua.

local cassandra = require "cassandra"
local log = require "cassandra.log"

-- Define log level for tests
log.set_lvl("ERR")

local _shm = "cassandra_specs"
local _contact_points = {"127.0.0.1", "127.0.0.2"}

describe("spawn cluster", function()
  it("should require a 'shm' option", function()
    assert.has_error(function()
      cassandra.spawn_cluster({
        shm = nil,
        contact_points = _contact_points
      })
    end, "shm is required for spawning a cluster/session")
  end)
  it("should spawn a cluster", function()
    local ok, err = cassandra.spawn_cluster({
      shm = _shm,
      contact_points = _contact_points
    })
    assert.falsy(err)
    assert.True(ok)
  end)
  it("should retrieve cluster infos in spawned cluster's shm", function()
    local cache = require "cassandra.cache"
    local hosts, err = cache.get_hosts(_shm)
    assert.falsy(err)
    -- index of hosts
    assert.equal(3, #hosts)
    -- hosts details
    for _, host_addr in ipairs(hosts) do
      local host_details = cache.get_host(_shm, host_addr)
      assert.truthy(host_details)
    end
  end)
  it("should iterate over contact_points to find an entrance into the cluster", function()
    local contact_points = {"0.0.0.1", "0.0.0.2", "0.0.0.3"}
    contact_points[#contact_points + 1] = _contact_points[1]

    local ok, err = cassandra.spawn_cluster({
      shm = "test",
      contact_points = contact_points
    })
    assert.falsy(err)
    assert.True(ok)
  end)
  it("should return an error when no contact_point is valid", function()
    local contact_points = {"0.0.0.1", "0.0.0.2", "0.0.0.3"}
    local ok, err = cassandra.spawn_cluster({
      shm = "test",
      contact_points = contact_points
    })
    assert.truthy(err)
    assert.False(ok)
    assert.equal("NoHostAvailableError", err.type)
    assert.equal("All hosts tried for query failed. 0.0.0.1: No route to host. 0.0.0.2: No route to host. 0.0.0.3: No route to host.", err.message)
  end)
  it("should accept a custom port for given hosts", function()
    local contact_points = {}
    for i, addr in ipairs(_contact_points) do
      contact_points[i] = addr..":9043"
    end
    local ok, err = cassandra.spawn_cluster({
      shm = "test",
      contact_points = contact_points
    })
    assert.truthy(err)
    assert.False(ok)
    assert.equal("NoHostAvailableError", err.type)
  end)
end)

describe("spawn session", function()
  local session
  it("should require a 'shm' option", function()
    assert.has_error(function()
      cassandra.spawn_session({
        shm = nil
      })
    end, "shm is required for spawning a cluster/session")
  end)
  it("should spawn a session", function()
    local err
    session, err = cassandra.spawn_session({
      shm = _shm
    })
    assert.falsy(err)
    assert.truthy(session)
  end)
  describe(":execute()", function()
    describe("ROWS parsing", function()
      it("should execute a SELECT query, parsing ROWS", function()
        local rows, err = session:execute("SELECT key FROM system.local")
        assert.falsy(err)
        assert.truthy(rows)
        assert.equal("ROWS", rows.type)
        assert.equal(1, #rows)
        assert.equal("local", rows[1].key)
      end)
      it("should accept query arguments", function()
        local rows, err = session:execute("SELECT key FROM system.local WHERE key = ?", {"local"})
        assert.falsy(err)
        assert.truthy(rows)
        assert.equal("ROWS", rows.type)
        assert.equal(1, #rows)
        assert.equal("local", rows[1].key)
      end)
    end)
    describe("SCHEMA_CHANGE/SET_KEYSPACE parsing", function()
      local res, err = session:execute [[
        CREATE KEYSPACE IF NOT EXISTS resty_cassandra_spec_parsing
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
      ]]
      assert.falsy(err)
      assert.truthy(res)
      assert.equal(0, #res)
      assert.equal("SCHEMA_CHANGE", res.type)
      assert.equal("CREATED", res.change)
      assert.equal("KEYSPACE", res.keyspace)
      assert.equal("resty_cassandra_spec_parsing", res.table)

      res, err = session:execute [[USE "resty_cassandra_spec_parsing"]]
      assert.falsy(err)
      assert.truthy(res)
      assert.equal(0, #res)
      assert.equal("SET_KEYSPACE", res.type)
      assert.equal("resty_cassandra_spec_parsing", res.keyspace)

      res, err = session:execute("DROP KEYSPACE resty_cassandra_spec_parsing")
      assert.falsy(err)
      assert.truthy(res)
      assert.equal(0, #res)
      assert.equal("DROPPED", res.change)
    end)
  end)
end)

describe("use case", function()
  local session

  setup(function()
    local err
    session, err = cassandra.spawn_session {
      shm = _shm
    }
    assert.falsy(err)

    local _, err = session:execute [[
      CREATE KEYSPACE IF NOT EXISTS resty_cassandra_specs
      WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    ]]
    assert.falsy(err)

    os.execute("sleep 1")

    local _, err = session:execute [[
      CREATE TABLE IF NOT EXISTS resty_cassandra_specs.users(
         id uuid PRIMARY KEY,
         name varchar,
         age int
      )
    ]]
    assert.falsy(err)
  end)

  teardown(function()
    local _, err = session:execute("DROP KEYSPACE resty_cassandra_specs")
    assert.falsy(err)

    session:close()
  end)

  describe(":set_keyspace()", function()
    it("should set a session's 'keyspace' option", function()
      session:set_keyspace("resty_cassandra_specs")
      assert.equal("resty_cassandra_specs", session.options.keyspace)

      local rows, err = session:execute("SELECT * FROM users")
      assert.falsy(err)
      assert.equal(0, #rows)
    end)
  end)

  describe(":execute()", function()
    it("should accept values to bind", function()
      local res, err = session:execute("INSERT INTO users(id, name, age) VALUES(?, ?, ?)",
        {cassandra.types.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93"), "Bob", 42})
      assert.falsy(err)
      assert.truthy(res)
      assert.equal("VOID", res.type)
    end)
  end)
end)
