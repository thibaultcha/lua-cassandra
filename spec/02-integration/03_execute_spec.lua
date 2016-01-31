local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

describe("execute()", function()
  local session

  setup(function()
    local err
    local _hosts, _shm = utils.ccm_start()

    session, err = cassandra.spawn_session {
      shm = _shm,
      contact_points = _hosts
    }
    assert.falsy(err)
  end)

  it("should require argument #1 to be a string", function()
    assert.has_error(function()
      session:execute()
    end, "argument #1 must be a string")
  end)

  --
  -- RESULT PARSING
  --
  describe("result types", function()
    after_each(function()
      -- drop keyspace in case a test failed
      utils.drop_keyspace(session, "res_types")
    end)

    it("should parse ROWS results", function()
      local rows, err = session:execute "SELECT key FROM system.local"
      assert.falsy(err)
      assert.is_table(rows)
      assert.equal("ROWS", rows.type)
      assert.equal(1, #rows)
      assert.equal("local", rows[1].key)
    end)
    it("should return ROWS results with a `meta` property", function()
      local rows, err = session:execute("SELECT * FROM system.local")
      assert.falsy(err)
      assert.is_table(rows)
      assert.is_table(rows.meta)
      assert.falsy(rows.meta.columns)
      assert.falsy(rows.meta.columns_count)
      assert.is_boolean(rows.meta.has_more_pages)
    end)
    it("should parse SCHEMA_CHANGE -> CREATED result", function()
      local res, err = session:execute [[
        CREATE KEYSPACE IF NOT EXISTS res_types
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
      ]]
      assert.falsy(err)
      assert.is_table(res)
      assert.equal(0, #res)
      assert.equal("SCHEMA_CHANGE", res.type)
      assert.equal("CREATED", res.change)
      assert.equal("KEYSPACE", res.keyspace)
      assert.equal("res_types", res.table)
    end)
    it("should parse SET_KEYSPACE results", function()
      local res, err = session:execute [[
        CREATE KEYSPACE IF NOT EXISTS res_types
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
      ]]
      assert.falsy(err)
      assert.is_table(res)

      res, err = session:execute [[USE "res_types"]]
      assert.falsy(err)
      assert.is_table(res)
      assert.equal(0, #res)
      assert.equal("SET_KEYSPACE", res.type)
      assert.equal("res_types", res.keyspace)
    end)
    it("should parse SCHEMA_CHANGE -> DROPPED result", function()
      local res, err = session:execute [[
        CREATE KEYSPACE IF NOT EXISTS res_types
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
      ]]
      assert.falsy(err)
      assert.is_table(res)

      res, err = session:execute "DROP KEYSPACE res_types"
      assert.falsy(err)
      assert.is_table(res)
      assert.equal(0, #res)
      assert.equal("DROPPED", res.change)
    end)
  end)

  describe("", function()
    setup(function()
      utils.create_keyspace(session, "execute")

      local res, err = session:execute [[
        CREATE TABLE IF NOT EXISTS execute.users(
          id uuid,
          name varchar,
          n int,
          PRIMARY KEY(id, n)
        )
      ]]
      assert.falsy(err)
      assert.truthy(res)

      res, err = session:set_keyspace "execute"
      assert.falsy(err)
      assert.True(res)
    end)

    before_each(function()
      for i = 1, utils.n_inserts do
        local res, err = session:execute([[
          INSERT INTO users(id, name, n) VALUES(2644bada-852c-11e3-89fb-e0b9a54a6d93, ?, ?)
        ]], {"Alice", i})

        assert.falsy(err)
        assert.is_table(res)
      end
    end)

    after_each(function()
      session:execute("TRUNCATE users")
    end)

    it("should have inserted "..utils.n_inserts, function()
      local rows, err = session:execute("SELECT COUNT(*) FROM users")
      assert.falsy(err)
      assert.is_table(rows)
      assert.equal(utils.n_inserts, rows[1].count)
    end)

    --
    -- ARGS BINDING
    --
    describe("args binding", function()
      it("should accept values to bind", function()
        local res, err = session:execute([[
          INSERT INTO users(id, name, n) VALUES(?, ?, ?)
        ]], {
          cassandra.uuid("4444bada-852c-11e3-89fb-e0b9a54a6d94"),
          "Bob",
          1
        })
        assert.falsy(err)
        assert.is_table(res)
        assert.equal("VOID", res.type)

        local rows, err = session:execute [[
          SELECT * FROM users WHERE id = 4444bada-852c-11e3-89fb-e0b9a54a6d94
        ]]
        assert.falsy(err)
        assert.is_table(rows)
        assert.equal(1, #rows)
        assert.equal("Bob", rows[1].name)
      end)
    end)

    --
    -- PAGINATION
    --
    describe("pagination", function()
      it("should have a default page_size (1000)", function()
        for i = utils.n_inserts + 1, 1001 do
          local res, err = session:execute([[
            INSERT INTO users(id, name, n) VALUES(2644bada-852c-11e3-89fb-e0b9a54a6d93, ?, ?)
          ]], { "Alice", i})

          assert.falsy(err)
          assert.is_table(res)
        end

        local rows, err = session:execute [[
          SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93 ORDER BY n
        ]]
        assert.falsy(err)
        assert.is_table(rows)
        assert.is_table(rows.meta)
        assert.True(rows.meta.has_more_pages)
        assert.truthy(rows.meta.paging_state)
        assert.equal(1000, #rows)
        assert.equal(1, rows[1].n)
        assert.equal(1000, rows[#rows].n)
      end)
      it("should be possible to specify a per-query page_size option", function()
        for i = utils.n_inserts, 1000 do
          local res, err = session:execute([[
            INSERT INTO users(id, name, n) VALUES(2644bada-852c-11e3-89fb-e0b9a54a6d93, ?, ?)
          ]], {"Alice", i})

          assert.falsy(err)
          assert.is_table(res)
        end

        local half = utils.n_inserts/2
        local rows, err = session:execute([[
          SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93 ORDER BY n
        ]], nil, {page_size = half})

        assert.falsy(err)
        assert.is_table(rows)
        assert.equal(half, #rows)

        local rows, err = session:execute("SELECT * FROM users")
        assert.falsy(err)
        assert.is_table(rows)
        assert.equal(1000, #rows) -- back to the default
      end)
      it("should support passing a paging_state to retrieve next pages", function()
        local half = utils.n_inserts/2
        local rows, err = session:execute([[
          SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93 ORDER BY n
        ]], nil, {page_size = half})

        assert.falsy(err)
        assert.is_table(rows)
        assert.equal(half, #rows)
        assert.equal(1, rows[1].n)
        assert.equal(half, rows[#rows].n)

        local paging_state = rows.meta.paging_state

        rows, err = session:execute([[
          SELECT * FROM users WHERE id = 2644bada-852c-11e3-89fb-e0b9a54a6d93 ORDER BY n
        ]], nil, {
          page_size = half,
          paging_state = paging_state
        })

        assert.falsy(err)
        assert.is_table(rows)
        assert.equal(half, #rows)
        assert.equal(half + 1, rows[1].n)
        assert.equal(utils.n_inserts, rows[#rows].n)
      end)
      describe("auto_paging", function()
        it("should return an iterator if given an `auto_paging` option", function()
          local page_tracker = 0
          for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = utils.n_inserts/10, auto_paging = true}) do
            assert.falsy(err)
            page_tracker = page_tracker + 1
            assert.equal(page_tracker, page)
            assert.is_table(rows)
            assert.equal(utils.n_inserts/10, #rows)
          end

          assert.equal(10, page_tracker)
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
            assert.is_table(rows)
            assert.equal(utils.n_inserts, #rows)
          end

          assert.equal(1, page_tracker)
        end)
        it("should return any error", function()
          -- This test validates the behaviour of err being returned if no
          -- results are returned (most likely because of an invalid query)
          local page_tracker = 0
          for rows, err, page in session:execute("SELECT * FROM users WHERE col = 500", nil, {auto_paging = true}) do
            assert.truthy(err) -- 'col' is not a valid column
            assert.same({meta = {has_more_pages = false}}, rows)
            assert.equal(0, page)
            page_tracker = page_tracker + 1
          end

          -- Assert the loop has been run once.
          assert.equal(1, page_tracker)
        end)
      end)
    end)

    --
    -- PREPARED
    --
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
        assert.is_table(rows)
        assert.True(#rows > 0)

        assert.spy(cache.get_prepared_query_id).was.called()
        assert.spy(cache.set_prepared_query_id).was.called()
        cache.get_prepared_query_id:clear()
        cache.set_prepared_query_id:clear()

        -- again, and this time the query_id should be in the cache already
        rows, err = session:execute("SELECT * FROM users", nil, {prepare = true})
        assert.falsy(err)
        assert.is_table(rows)
        assert.True(#rows > 0)

        assert.spy(cache.get_prepared_query_id).was.called()
        assert.spy(cache.set_prepared_query_id).was.not_called()
      end)
      it("should support a heavier load of prepared queries", function()
        for i = 1, utils.n_inserts do
          local rows, err = session:execute("SELECT * FROM users", nil, {
            prepare = true,
            page_size = 10
          })
          assert.falsy(err)
          assert.is_table(rows)
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
        for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = utils.n_inserts/10, auto_paging = true, prepare = true}) do
          assert.falsy(err)
          assert.is_table(rows)
          assert.True(#rows > 0 and #rows <= utils.n_inserts/10)
          page_tracker = page
        end

        assert.equal(10, page_tracker)
        assert.spy(cache.get_prepared_query_id).was.called(page_tracker + 1)
        assert.spy(cache.set_prepared_query_id).was.called(0)
      end)
    end)
  end)
end)
