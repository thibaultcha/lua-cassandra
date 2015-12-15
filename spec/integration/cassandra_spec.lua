--- Pure Lua integration tests.
-- lua-cassandra is built with support for pure Lua, outside of ngx_lua,
-- with fallback on LuaSocket when it is the case. Those integration tests must
-- mimic the ones running in ngx_lua.

local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

local LOG_LVL = "ERR"

-- Define log level for tests
cassandra.set_log_level(LOG_LVL)

local _shm = "cassandra_specs"
local _hosts = utils.hosts

describe("spawn_cluster()", function()
  it("should spawn a cluster", function()
    local ok, err = cassandra.spawn_cluster {
      shm = _shm,
      contact_points = _hosts
    }
    assert.falsy(err)
    assert.True(ok)
  end)
  it("should retrieve cluster infos in spawned cluster's shm", function()
    local cache = require "cassandra.cache"
    local hosts, err = cache.get_hosts(_shm)
    assert.falsy(err)
    -- index of hosts
    assert.equal(#_hosts, #hosts)
    -- hosts details
    for _, host_addr in ipairs(hosts) do
      local host_details = cache.get_host(_shm, host_addr)
      assert.truthy(host_details)
    end
  end)
  it("should iterate over contact_points to find an entrance into the cluster", function()
    cassandra.set_log_level("QUIET")
    finally(function()
      cassandra.set_log_level(LOG_LVL)
    end)

    local contact_points = {"0.0.0.1", "0.0.0.2", "0.0.0.3"}
    contact_points[#contact_points + 1] = _hosts[1]

    local ok, err = cassandra.spawn_cluster({
      shm = "test",
      contact_points = contact_points
    })
    assert.falsy(err)
    assert.True(ok)
  end)
  it("should accept a custom port for given hosts", function()
    cassandra.set_log_level("QUIET")
    finally(function()
      cassandra.set_log_level(LOG_LVL)
    end)

    local contact_points = {}
    for i, addr in ipairs(_hosts) do
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
    cassandra.set_log_level("QUIET")
    finally(function()
      cassandra.set_log_level(LOG_LVL)
    end)

    local ok, err = cassandra.spawn_cluster({
      shm = "test",
      protocol_options = {default_port = 9043},
      contact_points = _hosts
    })
    assert.truthy(err)
    assert.False(ok)
    assert.equal("NoHostAvailableError", err.type)
  end)
end)

describe("spawn_session()", function()
  local session
  it("should spawn a session", function()
    local err
    session, err = cassandra.spawn_session {shm = _shm}
    assert.falsy(err)
    assert.truthy(session)
    assert.truthy(session.hosts)
    assert.equal(#_hosts, #session.hosts)
  end)
  it("should spawn a session without having to spawn a cluster", function()
    local shm = "session_without_cluster"
    local session, err = cassandra.spawn_session {
      shm = shm,
      contact_points = _hosts
    }
    assert.falsy(err)
    assert.truthy(session)
    -- Check cache
    local cache = require "cassandra.cache"
    local hosts, err = cache.get_hosts(shm)
    assert.falsy(err)
    -- index of hosts
    assert.equal(#_hosts, #hosts)
    -- hosts details
    for _, host_addr in ipairs(hosts) do
      local host_details = cache.get_host(shm, host_addr)
      assert.truthy(host_details)
    end
  end)
  describe("execute()", function()
    teardown(function()
      -- drop keyspace in case tests failed
      session:execute("DROP KEYSPACE resty_cassandra_spec_parsing")
    end)
    it("should require argument #1 to be a string", function()
      assert.has_error(function()
        session:execute()
      end, "argument #1 must be a string")
    end)
    it("should parse ROWS results", function()
      local rows, err = session:execute("SELECT key FROM system.local")
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal("ROWS", rows.type)
      assert.equal(1, #rows)
      assert.equal("local", rows[1].key)
    end)
    it("should parse SCHEMA_CHANGE/SET_KEYSPACE results and wait for schema consensus", function()
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
  end)

  teardown(function()
    -- drop keyspace in case tests failed
    local err
    session, err = cassandra.spawn_session {shm = _shm}
    assert.falsy(err)

    utils.drop_keyspace(session, _KEYSPACE)
    session:shutdown()
  end)

  describe("set_keyspace()", function()
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

  describe("execute()", function()
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
    it("should return results with a `meta` property", function()
      local rows, err = session:execute("SELECT * FROM users")
      assert.falsy(err)
      assert.truthy(rows)
      assert.truthy(rows.meta)
      assert.falsy(rows.meta.columns)
      assert.falsy(rows.meta.columns_count)
      assert.False(rows.meta.has_more_pages)
    end)
    it("support somewhat heavier insertions", function()
      for i = 2, utils.n_inserts do
        local res, err = session:execute("INSERT INTO users(id, name, n) VALUES(2644bada-852c-11e3-89fb-e0b9a54a6d93, ?, ?)", {"Alice", i})
        assert.falsy(err)
        assert.truthy(res)
      end

      local rows, err = session:execute("SELECT COUNT(*) FROM users")
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(utils.n_inserts, rows[1].count)
    end)
    it("should have a default page_size (1000)", function()
      local rows, err = session:execute("SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93 ORDER BY n")
      assert.falsy(err)
      assert.truthy(rows)
      assert.truthy(rows.meta)
      assert.True(rows.meta.has_more_pages)
      assert.truthy(rows.meta.paging_state)
      assert.equal(1000, #rows)
      assert.equal(1, rows[1].n)
      assert.equal(1000, rows[#rows].n)
    end)
    it("should be possible to specify a per-query page_size option", function()
      local rows, err = session:execute("SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93 ORDER BY n", nil, {page_size = 100})
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(100, #rows)

      local rows, err = session:execute("SELECT * FROM users")
      assert.falsy(err)
      assert.truthy(rows)
      assert.equal(1000, #rows) -- back to the default
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

        assert.equal(utils.n_inserts/10, page_tracker)
      end)
      it("should return the latest page of a set", function()
        -- When the latest page contains only 1 element
        local page_tracker = 0
        for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = utils.n_inserts - 1, auto_paging = true}) do
          assert.falsy(err)
          page_tracker = page_tracker + 1
          assert.equal(page_tracker, page)
        end

        assert.equal(2, page_tracker)

        -- Even if all results are fetched in the first page
        page_tracker = 0
        for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = utils.n_inserts, auto_paging = true}) do
          assert.falsy(err)
          page_tracker = page_tracker + 1
          assert.equal(page_tracker, page)
          assert.equal(utils.n_inserts, #rows)
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
    describe("prepared queries", function()
      it("should prepare a query before running it if given a `prepare` option", function()
        local cache = require "cassandra.cache"
        spy.on(cache, "get_prepared_query_id")
        spy.on(cache, "set_prepared_query_id")
        finally(function()
          cache.get_prepared_query_id:revert()
          cache.set_prepared_query_id:revert()
        end)

        local rows, err = session:execute("SELECT * FROM users", nil, {prepare = true})
        assert.falsy(err)
        assert.truthy(rows)
        assert.True(#rows > 0)

        assert.spy(cache.get_prepared_query_id).was.called()
        assert.spy(cache.set_prepared_query_id).was.called()
        cache.get_prepared_query_id:clear()
        cache.set_prepared_query_id:clear()

        -- again, and this time the query_id should be in the cache already
        rows, err = session:execute("SELECT * FROM users", nil, {prepare = true})
        assert.falsy(err)
        assert.truthy(rows)
        assert.True(#rows > 0)

        assert.spy(cache.get_prepared_query_id).was.called()
        assert.spy(cache.set_prepared_query_id).was.not_called()
      end)
      it("should support a heavier load of prepared queries", function()
        for i = 1, utils.n_inserts do
          local rows, err = session:execute("SELECT * FROM users", nil, {prepare = false, page_size = 10})
          assert.falsy(err)
          assert.truthy(rows)
          assert.True(#rows > 0)
        end
      end)
      it("should be usable inside an `auto_paging` iterator", function()
        local cache = require "cassandra.cache"
        spy.on(cache, "get_prepared_query_id")
        spy.on(cache, "set_prepared_query_id")
        finally(function()
          cache.get_prepared_query_id:revert()
          cache.set_prepared_query_id:revert()
        end)

        local page_tracker = 1
        for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = 10, auto_paging = true, prepare = true}) do
          assert.falsy(err)
          assert.truthy(rows)
          assert.True(#rows > 0 and #rows <= 10)
          page_tracker = page
        end

        assert.equal(utils.n_inserts/10, page_tracker)
        assert.spy(cache.get_prepared_query_id).was.called(page_tracker + 1)
        assert.spy(cache.set_prepared_query_id).was.called(0)
      end)
    end)
  end)

  describe("batch()", function()
    local _UUID = "ca002f0a-8fe4-11e5-9663-43d80ec97d3e"

    setup(function()
      local _, err = session:execute [[
        CREATE TABLE IF NOT EXISTS counter_test_table(
          key text PRIMARY KEY,
          value counter
        )
      ]]
      assert.falsy(err)
    end)

    it("should execute logged batched queries with no params", function()
      local res, err = session:batch({
        {"INSERT INTO users(id, name, n) VALUES(".._UUID..", 'Alice', 1)"},
        {"UPDATE users SET name = 'Alice' WHERE id = ".._UUID.." AND n = 1"},
        {"UPDATE users SET name = 'Alicia' WHERE id = ".._UUID.." AND n = 1"}
      })
      assert.falsy(err)
      assert.truthy(res)
      assert.equal("VOID", res.type)

      local rows, err = session:execute("SELECT * FROM users WHERE id = ? AND n = 1", {cassandra.uuid(_UUID)})
      assert.falsy(err)
      assert.truthy(rows)
      local row = rows[1]
      assert.equal("Alicia", row.name)
    end)
    it("should execute logged batched queries with params", function()
      local res, err = session:batch({
        {"INSERT INTO users(id, name, n) VALUES(?, ?, ?)", {cassandra.uuid(_UUID), "Alice", 2}},
        {"UPDATE users SET name = ? WHERE id = ? AND n = 2", {"Alice", cassandra.uuid(_UUID)}},
        {"UPDATE users SET name = ? WHERE id = ? AND n = 2", {"Alicia2", cassandra.uuid(_UUID)}}
      })
      assert.falsy(err)
      assert.truthy(res)
      assert.equal("VOID", res.type)

      local rows, err = session:execute("SELECT * FROM users WHERE id = ? AND n = 2", {cassandra.uuid(_UUID)})
      assert.falsy(err)
      assert.truthy(rows)
      local row = rows[1]
      assert.equal("Alicia2", row.name)
    end)
    it("should execute unlogged batches", function()
      local res, err = session:batch({
        {"INSERT INTO users(id, name, n) VALUES(?, ?, ?)", {cassandra.uuid(_UUID), "Alice", 3}},
        {"UPDATE users SET name = ? WHERE id = ? AND n = 3", {"Alice", cassandra.uuid(_UUID)}},
        {"UPDATE users SET name = ? WHERE id = ? AND n = 3", {"Alicia3", cassandra.uuid(_UUID)}}
      }, {logged = false})
      assert.falsy(err)
      assert.truthy(res)
      assert.equal("VOID", res.type)

      local rows, err = session:execute("SELECT * FROM users WHERE id = ? AND n = 3", {cassandra.uuid(_UUID)})
      assert.falsy(err)
      assert.truthy(rows)
      local row = rows[1]
      assert.equal("Alicia3", row.name)
    end)
    it("should execute counter batches", function()
      local res, err = session:batch({
        {"UPDATE counter_test_table SET value = value + 1 WHERE key = 'counter'"},
        {"UPDATE counter_test_table SET value = value + 1 WHERE key = 'counter'"},
        {"UPDATE counter_test_table SET value = value + 1 WHERE key = ?", {"counter"}}
      }, {counter = true})
      assert.falsy(err)
      assert.truthy(res)
      assert.equal("VOID", res.type)

      local rows, err = session:execute("SELECT value FROM counter_test_table WHERE key = 'counter'")
      assert.falsy(err)
      assert.truthy(rows)
      local row = rows[1]
      assert.equal(3, row.value)
    end)
    it("should return any error", function()
      local _, err = session:batch({
        {"INSERT WHATEVER"},
        {"INSERT THING"}
      })
      assert.truthy(err)
      assert.equal("ResponseError", err.type)
    end)
    it("should support protocol level timestamp", function()
      local _, err = session:batch({
        {"INSERT INTO users(id, name, n) VALUES(".._UUID..", 'Alice', 4)"},
        {"UPDATE users SET name = 'Alice' WHERE id = ".._UUID.." AND n = 4"},
        {"UPDATE users SET name = 'Alicia4' WHERE id = ".._UUID.." AND n = 4"}
      }, {timestamp = 1428311323417123})
      assert.falsy(err)

      local rows, err = session:execute("SELECT name, writetime(name) FROM users WHERE id = ".._UUID.." AND n = 4")
      assert.falsy(err)
      assert.truthy(rows)
      local row = rows[1]
      assert.equal("Alicia4", row.name)
      assert.equal(1428311323417123, row["writetime(name)"])
    end)
    it("should support serial consistency", function()
      local _, err = session:batch({
        {"INSERT INTO users(id, name, n) VALUES(".._UUID..", 'Alice', 5)"},
        {"UPDATE users SET name = 'Alice' WHERE id = ".._UUID.." AND n = 5"},
        {"UPDATE users SET name = 'Alicia5' WHERE id = ".._UUID.." AND n = 5"}
      }, {serial_consistency = cassandra.consistencies.local_serial})
      assert.falsy(err)

      local rows, err = session:execute("SELECT name, writetime(name) FROM users WHERE id = ".._UUID.." AND n = 5")
      assert.falsy(err)
      assert.truthy(rows)
      local row = rows[1]
      assert.equal("Alicia5", row.name)
    end)
    it("should support prepared queries in batch", function()
      local cache = require "cassandra.cache"
      spy.on(cache, "get_prepared_query_id")
      spy.on(cache, "set_prepared_query_id")
      finally(function()
        cache.get_prepared_query_id:revert()
        cache.set_prepared_query_id:revert()
      end)

      local _, err = session:batch({
        {"INSERT INTO users(id, name, n) VALUES(?, ?, ?)", {cassandra.uuid(_UUID), "Alice", 6}},
        {"INSERT INTO users(id, name, n) VALUES(?, ?, ?)", {cassandra.uuid(_UUID), "Alice", 7}},
        {"UPDATE users SET name = ? WHERE id = ? AND n = ?", {"Alicia", cassandra.uuid(_UUID), 6}},
        {"UPDATE users SET name = ? WHERE id = ? AND n = ?", {"Alicia", cassandra.uuid(_UUID), 7}},
        {"UPDATE users SET name = ? WHERE id = ? AND n = ?", {"Alicia", cassandra.uuid(_UUID), 6}},
        {"UPDATE users SET name = ? WHERE id = ? AND n = ?", {"Alicia", cassandra.uuid(_UUID), 7}},
        {"UPDATE users SET name = ? WHERE id = ? AND n = ?", {"Alicia6", cassandra.uuid(_UUID), 6}},
        {"UPDATE users SET name = ? WHERE id = ? AND n = ?", {"Alicia7", cassandra.uuid(_UUID), 7}}
      }, {prepare = true})
      assert.falsy(err)

      assert.spy(cache.get_prepared_query_id).was.called(8)
      assert.spy(cache.set_prepared_query_id).was.called(2)

      local rows, err = session:execute("SELECT name FROM users WHERE id = ? AND n = ?", {cassandra.uuid(_UUID), 6})
      assert.falsy(err)
      assert.truthy(rows)
      local row = rows[1]
      assert.equal("Alicia6", row.name)
    end)
  end)

  describe("shutdown()", function()
    it("should close all connection and make the session unusable", function()
      session:shutdown()
      assert.True(session.terminated)
      assert.same({}, session.hosts)
      local rows, err = session:execute("SELECT * FROM users")
      assert.truthy(err)
      assert.equal("NoHostAvailableError", err.type)
      assert.falsy(rows)
    end)
  end)

  describe("set_keep_alive()", function()
    it("should fallback to close() when outside of ngx_lua", function()
      local session, err = cassandra.spawn_session {
        shm = _shm,
        contact_points = _hosts
      }
      assert.falsy(err)

      local _, err = session:execute("SELECT * FROM system.local")
      assert.falsy(err)

      assert.has_no_error(function()
        session:set_keep_alive()
      end)
    end)
  end)
end)
