local cassandra_v2 = require "cassandra.v2"
local cassandra_v3 = require "cassandra"

describe("Session", function()
for _, cass in ipairs({{v = "v2", c = cassandra_v2}, { v = "v3", c = cassandra_v3}}) do
local cassandra = cass.c

describe("Protocol #"..cass.v, function()
  describe(":new()", function()
    it("should instanciate a session", function()
      local session = cassandra:new()
      assert.truthy(session)
      assert.truthy(session.socket)
    end)
  end)

  describe(":set_keepalive()", function()
    it("should return an error if trying to use the cosocket API from luasocket", function()
      local session = cassandra:new()
      local err = select(2, session:set_keepalive())
      assert.equal("luasocket does not support reusable sockets", err.message)
    end)
  end)

  describe(":get_reused_times()", function()
    it("should return an error if trying to use the cosocket API from luasocket", function()
      local session = cassandra:new()
      local err = select(2, session:get_reused_times())
      assert.equal("luasocket does not support reusable sockets", err.message)
    end)
  end)

  describe(":connect()", function()
    it("should fail if no contact points are given", function()
      local session = cassandra:new()
      assert.has_error(function()
        session:connect()
      end, "no contact points provided")
    end)
    it("should connect if a contact point is given as a string", function()
      local session = cassandra:new()
      assert.has_no_error(function()
        local ok, err = session:connect("127.0.0.1")
        assert.falsy(err)
        assert.True(ok)
      end)
    end)
    it("should connect if some contact points are given as an array", function()
      local session = cassandra:new()
      assert.has_no_error(function()
        local ok, err = session:connect({"localhost", "127.0.0.1"})
        assert.falsy(err)
        assert.True(ok)
      end)
    end)
    it("should try another host if others fail", function()
      local session = cassandra:new()
      local ok, err = session:connect({"0.0.0.1", "0.0.0.2", "0.0.0.3", "127.0.0.1"})
      assert.falsy(err)
      assert.True(ok)
    end)
    it("should return error if it fails to connect to all hosts", function()
      local session = cassandra:new()
      local ok, err = session:connect({"0.0.0.1", "0.0.0.2", "0.0.0.3"})
      assert.False(ok)
      assert.truthy(err)
    end)
    it("should connect to a given port", function()
      local session = cassandra:new()
      local ok, err = session:connect("127.0.0.1", 9042)
      assert.falsy(err)
      assert.True(ok)
    end)
    it("should accept overriding the port for some hosts", function()
      -- If a contact point is of the form "host:port", this port will overwrite the one given as parameter of `connect`.
      local session = cassandra:new()
      local ok, err = session:connect({"127.0.0.1:9042"}, 9999)
      assert.True(ok)
      assert.falsy(err)
    end)
  end)

  describe(":close()", function()
    local session = cassandra:new()
    setup(function()
      local ok = session:connect("127.0.0.1")
      assert.True(ok)
    end)
    it("should close a connected session", function()
      local closed, err = session:close()
      assert.equal(1, closed)
      assert.falsy(err)
    end)
  end)

  describe(":execute()", function()
    local session = cassandra:new()
    local row
    setup(function()
      local ok = session:connect("127.0.0.1")
      assert.True(ok)
    end)
    teardown(function()
      session:close()
    end)
    it("should execute a query", function()
      local res, err = session:execute("SELECT cql_version, native_protocol_version, release_version FROM system.local")
      assert.falsy(err)
      assert.truthy(res)
      assert.equal("ROWS", res.type)
      assert.equal(1, #res)
      row = res[1]
    end)
    describe("result rows", function()
      it("should be accessible by index or column name", function()
        if not row then pending() end
        assert.equal(row[1], row.cql_version)
        assert.equal(row[2], row.native_protocol_version)
        assert.equal(row[3], row.release_version)
      end)
    end)
    describe("errors", function()
      it("should return a Cassandra error", function()
        local res, err = session:execute("DESCRIBE")
        assert.falsy(res)
        assert.equal("Cassandra returned error (Syntax_error): line 1:0 no viable alternative at input 'DESCRIBE' ([DESCRIBE])", tostring(err))
      end)
    end)
  end)

  describe("Prepared Statements", function()
    local session = cassandra:new()
    setup(function()
      local ok = session:connect("127.0.0.1")
      assert.True(ok)
    end)
    teardown(function()
      session:close()
    end)
    describe(":prepare()", function()
      it("should prepare a query", function()
        local stmt, err = session:prepare("SELECT native_protocol_version FROM system.local")
        assert.falsy(err)
        assert.truthy(stmt)
        assert.equal("PREPARED", stmt.type)
        assert.truthy(stmt.id)
      end)
      it("should prepare a query with tracing", function()
        local stmt, err = session:prepare("SELECT native_protocol_version FROM system.local", true)
        assert.falsy(err)
        assert.truthy(stmt)
        assert.equal("PREPARED", stmt.type)
        assert.truthy(stmt.id)
        assert.truthy(stmt.tracing_id)
      end)
      it("should prepare a query with binded parameters", function()
        local stmt, err = session:prepare("SELECT * FROM system.local WHERE key = ?")
        assert.falsy(err)
        assert.truthy(stmt)
        assert.equal("PREPARED", stmt.type)
        assert.truthy(stmt.id)
      end)
    end)
  end)

  describe("Functional use case", function()
    local session = cassandra:new()
    setup(function()
      local ok = session:connect("127.0.0.1")
      assert.True(ok)
      local _, err = session:execute [[
        CREATE KEYSPACE IF NOT EXISTS lua_cassandra_tests
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
      ]]
      assert.falsy(err)
    end)
    teardown(function()
      session:execute("DROP KEYSPACE lua_cassandra_tests")
      session:close()
    end)
    describe(":set_keyspace()", function()
      it("should set the session's keyspace", function()
        local res, err = session:set_keyspace("lua_cassandra_tests")
        assert.falsy(err)
        assert.truthy(res)
        assert.equal("SET_KEYSPACE", res.type)
        assert.equal("lua_cassandra_tests", res.keyspace)
      end)
    end)
    describe(":execute()", function()
      setup(function()
        local _, err = session:execute [[
          CREATE TABLE IF NOT EXISTS users(
            id uuid,
            name varchar,
            age int,
            PRIMARY KEY(id, age)
          )
        ]]
        assert.falsy(err)
      end)
      it("should execute with binded parameters", function()
        local res, err = session:execute([[ INSERT INTO users(id, name, age)
                                            VALUES(?, ?, ?)
          ]], {cassandra.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93"), "Bob", 42})
        assert.falsy(err)
        assert.truthy(res)
        assert.equal("VOID", res.type)
      end)
      it("should execute a prepared statement", function()
        local err, stmt, res
        stmt, err = session:prepare("SELECT * FROM users")
        assert.falsy(err)
        assert.truthy(stmt)

        res, err = session:execute(stmt)
        assert.falsy(err)
        assert.truthy(res)
        assert.equal("ROWS", res.type)
        assert.equal(1, #res)
        assert.equal("Bob", res[1].name)
        assert.equal(42, res[1].age)
      end)
      it("should execute a prepared statement with binded parameters", function()
        local err, stmt, res
        stmt, err = session:prepare("SELECT * FROM users WHERE id = ?")
        assert.falsy(err)
        assert.truthy(stmt)

        res, err = session:execute(stmt, {cassandra.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93")})
        assert.falsy(err)
        assert.truthy(res)
        assert.equal("ROWS", res.type)
        assert.equal(1, #res)
        assert.equal("Bob", res[1].name)
        assert.equal(42, res[1].age)
      end)
      describe("execute options", function()
        it("should be possible to query with tracing", function()
          local rows, err = session:execute("SELECT * FROM system.local", nil, {tracing = true})
          assert.falsy(err)
          assert.truthy(rows.tracing_id)
        end)
      it("should support the serial_consitency flag", function()
        -- serial_consistency only works for conditional update statements but
        -- we are here tracking the driver's behaviour when passing the flag
        local _, err = session:execute([[
          INSERT INTO users(id, age, name) VALUES(uuid(), 30, 'leo') IF NOT EXISTS
          ]], nil, {serial_consistency = cassandra.consistency.LOCAL_SERIAL})
        assert.falsy(err)
      end)
    end)
    describe("Pagination", function()
      setup(function()
        local err = select(2, session:execute("TRUNCATE users"))
        assert.falsy(err)
        for i = 1, 200 do
          err = select(2, session:execute("INSERT INTO users(id, name, age) VALUES(uuid(), ?, ?)",
          { "user"..i, i }))
          if err then error(err) end
        end
      end)
      it("should fetch everything given that the default page size is big enough", function()
        local res, err = session:execute("SELECT * FROM users")
        assert.falsy(err)
        assert.equal(200, #res)
      end)
      it("should support a page_size option", function()
        local rows, err = session:execute("SELECT * FROM users", nil, {page_size = 200})
        assert.falsy(err)
        assert.same(200, #rows)

        rows, err = session:execute("SELECT * FROM users", nil, {page_size = 100})
        assert.falsy(err)
        assert.same(100, #rows)
      end)
      it("should return metadata flags about pagination", function()
        local res, err = session:execute("SELECT * FROM users", nil, {page_size = 100})
        assert.falsy(err)
        assert.True(res.meta.has_more_pages)
        assert.truthy(res.meta.paging_state)

        -- Full page
        res, err = session:execute("SELECT * FROM users")
        assert.falsy(err)
        assert.False(res.meta.has_more_pages)
        assert.falsy(res.meta.paging_state)
      end)
      it("should fetch the next page if given a `paging_state` option", function()
        local res, err = session:execute("SELECT * FROM users", nil, {page_size = 100})
        assert.falsy(err)
        assert.equal(100, #res)

        res, err = session:execute("SELECT * FROM users", nil, {
          page_size = 100,
          paging_state = res.meta.paging_state
        })
        assert.falsy(err)
        assert.equal(100, #res)
      end)
      describe("auto_paging", function()
        it("should return an iterator if given an `auto_paging` options", function()
          local page_tracker = 0
          for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = 10, auto_paging = true}) do
            assert.falsy(err)
            page_tracker = page_tracker + 1
            assert.equal(page_tracker, page)
            assert.equal(10, #rows)
          end

          assert.equal(20, page_tracker)
        end)
        it("should return the latest page of a set", function()
          -- When the latest page contains only 1 element
          local page_tracker = 0
          for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = 199, auto_paging = true}) do
            assert.falsy(err)
            page_tracker = page_tracker + 1
            assert.equal(page_tracker, page)
          end

          assert.equal(2, page_tracker)

          -- Even if all results are fetched in the first page
          page_tracker = 0
          for rows, err, page in session:execute("SELECT * FROM users", nil, {auto_paging = true}) do
            assert.falsy(err)
            page_tracker = page_tracker + 1
            assert.equal(page_tracker, page)
            assert.equal(200, #rows)
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
  end) -- describe :execute()
  describe("BatchStatement", function()
    setup(function()
      local err = select(2, session:execute("TRUNCATE users"))
      assert.falsy(err)
    end)
    it("should instanciate a batch statement", function()
      local batch = cassandra:BatchStatement()
      assert.truthy(batch)
      assert.equal("table", type(batch.queries))
      assert.True(batch.is_batch_statement)
    end)
    it("should instanciate a logged batch by default", function()
      local batch = cassandra:BatchStatement()
      assert.equal(cassandra.batch_types.LOGGED, batch.type)
    end)
    it("should instanciate different types of batch", function()
      -- Unlogged
      local batch = cassandra:BatchStatement(cassandra.batch_types.UNLOGGED)
      assert.equal(cassandra.batch_types.UNLOGGED, batch.type)
      -- Counter
      batch = cassandra:BatchStatement(cassandra.batch_types.COUNTER)
      assert.equal(cassandra.batch_types.COUNTER, batch.type)
    end)
    it("should be possible to add queries to a batch", function()
      local batch = cassandra:BatchStatement()
      assert.has_no_error(function()
        batch:add("INSERT INTO users(id, name) VALUES(uuid(), ?)", {"Laura"})
        batch:add("INSERT INTO users(id, name) VALUES(uuid(), ?)", {"James"})
      end)
      assert.equal(2, #batch.queries)
    end)
    it("should be possible to execute a batch", function()
      local batch = cassandra:BatchStatement()
      batch:add("INSERT INTO users(id, age, name) VALUES(uuid(), ?, ?)", {21, "Laura"})
      batch:add("INSERT INTO users(id, age, name) VALUES(uuid(), ?, ?)", {22, "James"})

      local res, err = session:execute(batch)
      assert.falsy(err)
      assert.truthy(res)
      assert.equal("VOID", res.type)

      -- Check insertion
      res, err = session:execute("SELECT * FROM users")
      assert.falsy(err)
      assert.equal(2, #res)
    end)
    it("should execute unlogged batch statement", function()
      local batch = cassandra:BatchStatement(cassandra.batch_types.UNLOGGED)
      batch:add("INSERT INTO users(id, age, name) VALUES(uuid(), ?, ?)", {21, "Laura"})
      batch:add("INSERT INTO users(id, age, name) VALUES(uuid(), ?, ?)", {22, "James"})

      local res, err = session:execute(batch)
      assert.falsy(err)
      assert.truthy(res)
      assert.equal("VOID", res.type)

      -- Check insertion
      res, err = session:execute("SELECT * FROM users")
      assert.falsy(err)
      assert.equal(4, #res)
    end)
    describe("Counter batch", function()
      setup(function()
        local err = select(2, session:execute([[
          CREATE TABLE IF NOT EXISTS counter_test_table(
            key text PRIMARY KEY,
            value counter
          )
        ]]))
        assert.falsy(err)
      end)
      it("should execute counter batch statement", function()
        local batch = cassandra:BatchStatement(cassandra.batch_types.COUNTER)

        -- Query
        batch:add("UPDATE counter_test_table SET value = value + 1 WHERE key = 'key'")

        -- Binded queries
        batch:add("UPDATE counter_test_table SET value = value + 1 WHERE key = ?", {"key"})
        batch:add("UPDATE counter_test_table SET value = value + 1 WHERE key = ?", {"key"})

        -- Prepared statement
        local stmt, res, err
        stmt, err = session:prepare [[
          UPDATE counter_test_table SET value = value + 1 WHERE key = ?
        ]]
        assert.falsy(err)
          batch:add(stmt, {"key"})

          res, err = session:execute(batch)
          assert.falsy(err)
          assert.truthy(res)

          res, err = session:execute [[
            SELECT value from counter_test_table WHERE key = 'key'
          ]]
          assert.falsy(err)
          assert.equal(4, res[1].value)
        end)
      end)
    end)
  end) -- describe Functional Use Case
end) -- describe Protocol
end
end) -- describe Session

describe("Only v3", function()
  local session = cassandra_v3:new()
  setup(function()
    local ok = session:connect("127.0.0.1")
    assert.True(ok)
    local _, err = session:execute [[
      CREATE KEYSPACE IF NOT EXISTS lua_cassandra_tests
      WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2}
    ]]
    assert.falsy(err)
    session:set_keyspace("lua_cassandra_tests")
  end)
  teardown(function()
    session:execute("DROP KEYSPACE lua_cassandra_tests")
    session:close()
  end)
  describe("User Defined Type", function()
    setup(function()
      local err = select(2, session:execute([[
        CREATE TYPE address (
          street text,
          city text,
          zip int,
          country text
        )
      ]]))
      assert.falsy(err)

      err = select(2, session:execute([[
        CREATE TABLE user_profiles (
          email text PRIMARY KEY,
          address frozen<address>
        )
      ]]))
      assert.falsy(err)
    end)
    teardown(function()
      session:execute("DROP TYPE address")
      session:execute("DROP TABLE user_profiles")
    end)
    it("should be possible to insert and get value back", function()
      local rows, err
      err = select(2, session:execute([[
        INSERT INTO user_profiles(email, address) VALUES (?, ?)
      ]], {"email@domain.com", cassandra_v3.udt({ "montgomery street", "san francisco", 94111, nil })}))

      assert.falsy(err)

      rows, err = session:execute("SELECT address FROM user_profiles WHERE email = 'email@domain.com'")
      assert.falsy(err)
      assert.same(1, #rows)
      local row = rows[1]
      assert.same("montgomery street", row.address.street)
      assert.same("san francisco", row.address.city)
      assert.same(94111, row.address.zip)
      assert.same("", row.address.country)
    end)
  end)
end)
