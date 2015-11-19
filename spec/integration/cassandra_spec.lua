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
        id uuid,
        name varchar,
        n int,
        PRIMARY KEY(id, n)
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
      local res, err = session:execute("INSERT INTO users(id, name, n) VALUES(?, ?, ?)",
        {cassandra.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93"), "Bob", 1})
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
      for i = 2, 10000 do
        local res, err = session:execute("INSERT INTO users(id, name, n) VALUES(2644bada-852c-11e3-89fb-e0b9a54a6d93, ?, ?)", {"Alice", i})
        assert.falsy(err)
        assert.truthy(res)
      end

      local rows, err = session:execute("SELECT COUNT(*) FROM users")
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(10000, rows[1].count)
    end)
    it("should have a default page_size (5000)", function()
      local rows, err = session:execute("SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93 ORDER BY n")
      assert.falsy(err)
      assert.truthy(rows)
      assert.truthy(rows.meta)
      assert.True(rows.meta.has_more_pages)
      assert.truthy(rows.meta.paging_state)
      assert.equal(5000, #rows)
      assert.equal(1, rows[1].n)
      assert.equal(5000, rows[#rows].n)
    end)
    it("should be possible to specify a per-query page_size option", function()
      local rows, err = session:execute("SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93 ORDER BY n", nil, {page_size = 100})
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(100, #rows)

      local rows, err = session:execute("SELECT * FROM users")
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(5000, #rows)
    end)
    it("should support passing a paging_state to retrieve next pages", function()
      local rows, err = session:execute("SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93 ORDER BY n", nil, {page_size = 100})
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(100, #rows)
      assert.equal(1, rows[1].n)
      assert.equal(100, rows[#rows].n)

      local paging_state = rows.meta.paging_state

      rows, err = session:execute("SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93 ORDER BY n", nil, {page_size = 100, paging_state = paging_state})
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(100, #rows)
      assert.equal(101, rows[1].n)
      assert.equal(200, rows[#rows].n)
    end)
    describe("auto_paging", function()
      it("should return an iterator if given an `auto_paging` option", function()
        local page_tracker = 0
        for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = 10, auto_paging = true}) do
          assert.falsy(err)
          page_tracker = page_tracker + 1
          assert.equal(page_tracker, page)
          assert.equal(10, #rows)
        end

        assert.equal(1000, page_tracker)
      end)
      it("should return the latest page of a set", function()
        -- When the latest page contains only 1 element
        local page_tracker = 0
        for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = 9999, auto_paging = true}) do
          assert.falsy(err)
          page_tracker = page_tracker + 1
          assert.equal(page_tracker, page)
        end

        assert.equal(2, page_tracker)

        -- Even if all results are fetched in the first page
        page_tracker = 0
        for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = 10000, auto_paging = true}) do
          assert.falsy(err)
          page_tracker = page_tracker + 1
          assert.equal(page_tracker, page)
          assert.equal(10000, #rows)
        end

        assert.same(1, page_tracker)
      end)
      it("should return any error", function()
        -- This test validates the behaviour of err being returned if no
        -- results are returned (most likely because of an invalid query)
        local page_tracker = 0
        for rows, err, page in session:execute("SELECT * FROM users WHERE col = 500", nil, {auto_paging = true}) do
          assert.truthy(err) -- 'col' is not a valid column
          assert.equal(0, page)
          page_tracker = page_tracker + 1
        end

        -- Assert the loop has been run once.
        assert.equal(1, page_tracker)
      end)
    end)
  end)

  describe(":shutdown()", function()
    session:shutdown()
    assert.True(session.terminated)
    assert.same({}, session.hosts)
  end)
end)
