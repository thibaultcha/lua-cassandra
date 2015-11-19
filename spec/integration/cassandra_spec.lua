--- Pure Lua integration tests.
-- lua-cassandra is built with support for pure Lua, outside of ngx_lua,
-- with fallback on LuaSocket when it is the case. Those integration tests must
-- mimic the ones running in ngx_lua.

local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

-- Define log level for tests
utils.set_log_lvl("ERR")

local _shm = "cassandra_specs"

describe("spawn cluster", function()
  it("should require a 'shm' option", function()
    assert.has_error(function()
      cassandra.spawn_cluster({
        shm = nil,
        contact_points = utils.contact_points
      })
    end, "shm is required for spawning a cluster/session")
  end)
  it("should spawn a cluster", function()
    local ok, err = cassandra.spawn_cluster({
      shm = _shm,
      contact_points = utils.contact_points
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
    contact_points[#contact_points + 1] = utils.contact_points[1]

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
    for i, addr in ipairs(utils.contact_points) do
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
  it("should accept a custom port through an option", function()
    local ok, err = cassandra.spawn_cluster({
      shm = "test",
      protocol_options = {default_port = 9043},
      contact_points = utils.contact_points
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
    assert.truthy(session.hosts)
    assert.equal(3, #session.hosts)
  end)
  describe(":execute()", function()
    teardown(function()
      -- drop keyspace in case tests failed
      session:execute("DROP KEYSPACE resty_cassandra_spec_parsing")
    end)
    it("should parse ROWS results", function()
      local rows, err = session:execute("SELECT key FROM system.local")
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal("ROWS", rows.type)
      assert.equal(1, #rows)
      assert.equal("local", rows[1].key)
    end)
    it("should parse SCHEMA_CHANGE/SET_KEYSPACE results", function()
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

      utils.wait()

      res, err = session:execute [[USE "resty_cassandra_spec_parsing"]]
      assert.falsy(err)
      assert.truthy(res)
      assert.equal(0, #res)
      assert.equal("SET_KEYSPACE", res.type)
      assert.equal("resty_cassandra_spec_parsing", res.keyspace)
    end)
    it("should spawn a session in a given keyspace", function()
      local session_in_keyspace, err = cassandra.spawn_session({
        shm = _shm,
        keyspace = "resty_cassandra_spec_parsing"
      })
      assert.falsy(err)
      assert.equal("resty_cassandra_spec_parsing", session_in_keyspace.options.keyspace)
      assert.equal("resty_cassandra_spec_parsing", session_in_keyspace.hosts[1].options.keyspace)

      local _, err = session:execute [[
        CREATE TABLE IF NOT EXISTS resty_cassandra_spec_parsing.users(
          id uuid PRIMARY KEY,
          name varchar,
          age int
        )
      ]]
      assert.falsy(err)

      utils.wait()

      local rows, err = session_in_keyspace:execute("SELECT * FROM users")
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(0, #rows)
    end)
    it("should parse SCHEMA_CHANGE bis", function()
      local res, err = session:execute("DROP KEYSPACE resty_cassandra_spec_parsing")
      assert.falsy(err)
      assert.truthy(res)
      assert.equal(0, #res)
      assert.equal("DROPPED", res.change)
    end)
  end)
end)

describe("session", function()
  local session
  local _KEYSPACE = "resty_cassandra_specs"

  setup(function()
    local err
    session, err = cassandra.spawn_session {shm = _shm}
    assert.falsy(err)

    utils.create_keyspace(session, _KEYSPACE)

    local _, err = session:execute [[
      CREATE TABLE IF NOT EXISTS resty_cassandra_specs.users(
        id uuid PRIMARY KEY,
        name varchar,
        age int
      )
    ]]
    assert.falsy(err)

    utils.wait()
  end)

  teardown(function()
    -- drop keyspace in case tests failed
    local err
    session, err = cassandra.spawn_session {shm = _shm}
    assert.falsy(err)

    utils.drop_keyspace(session, _KEYSPACE)
    session:shutdown()
  end)

  describe(":set_keyspace()", function()
    it("should set a session's 'keyspace' option", function()
      local ok, err = session:set_keyspace(_KEYSPACE)
      assert.falsy(err)
      assert.True(ok)
      assert.equal(_KEYSPACE, session.options.keyspace)

      local rows, err = session:execute("SELECT * FROM users")
      assert.falsy(err)
      assert.equal(0, #rows)
    end)
  end)

  describe(":execute()", function()
    it("should accept values to bind", function()
      local res, err = session:execute("INSERT INTO users(id, name, age) VALUES(?, ?, ?)",
        {cassandra.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93"), "Bob", 42})
      assert.falsy(err)
      assert.truthy(res)
      assert.equal("VOID", res.type)

      local rows, err = session:execute("SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93")
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(1, #rows)
      assert.equal("Bob", rows[1].name)
    end)
    it("support somewhat heavier insertions", function()
      for i = 1, 1000 do
        local res, err = session:execute("INSERT INTO users(id, name, age) VALUES(uuid(), ?, ?)", {"Alice", 33})
        assert.falsy(err)
        assert.truthy(res)
      end

      local rows, err = session:execute("SELECT COUNT(*) FROM users")
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(1001, rows[1].count)
    end)
  end)

  describe(":shutdown()", function()
    session:shutdown()
    assert.True(session.terminated)
    assert.same({}, session.hosts)
  end)
end)
