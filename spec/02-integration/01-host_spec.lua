local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

describe("cassandra (host)", function()
  setup(function()
    utils.ccm_start(3)
  end)

  describe("consistencies", function()
    it("exposes Cassandra data consistencies", function()
      assert.is_table(cassandra.consistencies)

      local types = require "cassandra.types"
      for t in pairs(types.consistencies) do
        assert.truthy(cassandra.consistencies[t])
      end
    end)
  end)
  describe("cql_errors", function()
    it("exposes Cassandra CQL errors", function()
      assert.is_table(cassandra.cql_errors)

      local types = require "cassandra.types"
      for t in pairs(types.errors) do
        assert.truthy(cassandra.cql_errors[t])
      end
    end)
  end)
  describe("auth_providers", function()
    it("exposes default auth providers", function()
      assert.is_table(cassandra.auth_providers)
      assert.truthy(cassandra.auth_providers.plain_text)
    end)
  end)
  describe("shorthand serializers", function()
    it("throws an error on nil arg", function()
      assert.has_error(cassandra.uuid, "bad argument #1 to 'uuid' (got nil)")
      assert.has_error(cassandra.map, "bad argument #1 to 'map' (got nil)")
      assert.has_error(cassandra.list, "bad argument #1 to 'list' (got nil)")
      assert.has_error(cassandra.timestamp, "bad argument #1 to 'timestamp' (got nil)")
      local trace = debug.traceback()
      assert.matches("spec/02-integration/01-host_spec.lua", trace, nil, true)
    end)
  end)

  describe("new()", function()
    it("create a peer", function()
      local peer = assert(cassandra.new())
      assert.equal("127.0.0.1", peer.host)
      assert.equal(9042, peer.port)
      assert.equal(3, peer.protocol_version)
      assert.is_nil(peer.ssl)
      assert.truthy(peer.sock)
    end)
    it("accepts options", function()
      local peer = assert(cassandra.new {
        host = "192.168.1.1",
        port = 9043,
        protocol_version = 2
      })
      assert.equal("192.168.1.1", peer.host)
      assert.equal(9043, peer.port)
      assert.equal(2, peer.protocol_version)
      assert.is_nil(peer.ssl)
      assert.truthy(peer.sock)
    end)
  end)

  describe("__tostring()", function()
    it("has a __tostring() metamethod", function()
      local peer = assert(cassandra.new())
      assert.matches("<Cassandra socket: tcp{master}:", tostring(peer))
    end)
  end)

  describe("connect()", function()
    it("errors if not initialized", function()
      local ok, err = cassandra:connect()
      assert.equal("no socket created", err)
      assert.is_nil(ok)
    end)
    it("connects to a peer", function()
      local peer = assert(cassandra.new())
      assert(peer:connect())
    end)
  end)

  describe("close()", function()
    it("errors if not initialized", function()
      local ok, err = cassandra:close()
      assert.equal("no socket created", err)
      assert.is_nil(ok)
    end)
    it("closes a peer", function()
      local peer = assert(cassandra.new())
      assert(peer:connect())
      local ok = assert(peer:close())
      assert.equal(1, ok)
    end)
  end)

  describe("settimeout()", function()
    it("errors if not initialized", function()
      local ok, err = cassandra:settimeout()
      assert.equal("no socket created", err)
      assert.is_nil(ok)
    end)
    it("sets socket timeout", function()
      local peer = assert(cassandra.new())
      assert(peer:connect())

      assert.has_no_error(function()
        peer:settimeout(1000)
      end)

      finally(function()
        peer:close()
      end)
    end)
  end)

  describe("setkeepalive()", function()
    it("errors if not initialized", function()
      local ok, err = cassandra:setkeepalive()
      assert.equal("no socket created", err)
      assert.is_nil(ok)
    end)
    it("sets socket timeout", function()
      local peer = assert(cassandra.new())
      assert(peer:connect())
      assert(peer:setkeepalive())
      finally(function()
        peer:close()
      end)
    end)
  end)

  describe("CQL", function()
    local uuid, peer = "ca002f0a-8fe4-11e5-9663-43d80ec97d3e"
    setup(function()
      local p = assert(cassandra.new())
      assert(p:connect())
      assert(utils.create_keyspace(p, utils.keyspace))
      peer = p
    end)
    teardown(function()
      peer:close()
    end)

    describe("execute()", function()
      it("errors if not initialized", function()
        local ok, err = cassandra:execute()
        assert.equal("no socket created", err)
        assert.is_nil(ok)
      end)
      it("executes a CQL query", function()
        local rows = assert(peer:execute "SELECT * FROM system.local")
        assert.equal(1, #rows)
        assert.equal("ROWS", rows.type)

        local row = rows[1]
        assert.equal("local", row.key)
      end)
      it("parses ROWS results correctly", function()
        local rows = assert(peer:execute "SELECT * FROM system.local")
        assert.equal(1, #rows)
        assert.equal("ROWS", rows.type)
        assert.same({has_more_pages = false}, rows.meta)

        local row = rows[1]
        assert.equal("local", row.key)
      end)
      it("parses SCHEMA_CHANGE results", function()
        local tmp_name = os.tmpname():gsub("/", ""):lower()
        local res = assert(peer:execute([[
          CREATE KEYSPACE IF NOT EXISTS ]]..tmp_name..[[
          WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}
        ]]))
        assert.equal(0, #res)
        assert.equal("SCHEMA_CHANGE", res.type)
        assert.equal("CREATED", res.change_type)
        assert.equal("KEYSPACE", res.target)
        assert.equal(tmp_name, res.keyspace)
        assert.is_nil(res.name)

        res = assert(peer:execute([[
          CREATE TABLE ]]..tmp_name..[[.my_table(
            id uuid PRIMARY KEY,
            value int
          )
        ]]))
        assert.equal(0, #res)
        assert.equal("SCHEMA_CHANGE", res.type)
        assert.equal("CREATED", res.change_type)
        assert.equal("TABLE", res.target)
        assert.equal(tmp_name, res.keyspace)
        assert.equal("my_table", res.name)

        res = assert(peer:execute("DROP KEYSPACE "..tmp_name))
        assert.equal(0, #res)
        assert.equal("SCHEMA_CHANGE", res.type)
        assert.equal("DROPPED", res.change_type)
        assert.equal("KEYSPACE", res.target)
        assert.equal(tmp_name, res.keyspace)
        assert.is_nil(res.name)
      end)
      it("parses SET_KEYSPACE results", function()
        local peer_k = assert(cassandra.new())
        assert(peer_k:connect())

        local tmp_name = os.tmpname():gsub("/", ""):lower()
        local res = assert(peer_k:execute([[
          CREATE KEYSPACE IF NOT EXISTS ]]..tmp_name..[[
          WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}
        ]]))
        assert.equal("SCHEMA_CHANGE", res.type)

        res = assert(peer_k:execute(string.format('USE "%s"', tmp_name)))
        assert.equal(0, #res)
        assert.equal("SET_KEYSPACE", res.type)
        assert.equal(tmp_name, res.keyspace)

        res = assert(peer_k:execute("DROP KEYSPACE "..tmp_name))
        assert.equal("SCHEMA_CHANGE", res.type)
      end)
      it("returns CQL errors", function()
        local rows, err, code = peer:execute "SELECT"
        assert.is_nil(rows)
        assert.equal("[Syntax error] line 0:-1 no viable alternative at input '<EOF>'", err)
        assert.equal(cassandra.cql_errors.SYNTAX_ERROR, code)
      end)
      it("binds args", function()
        local rows = assert(peer:execute("SELECT * FROM system.local WHERE key = ?", {"local"}))
        assert.equal("local", rows[1].key)
      end)
    end) -- execute()

    describe("prepared queries", function()
      it("should prepare a query", function()
        local res = assert(peer:prepare "SELECT * FROM system.local WHERE key = ?")
        assert.truthy(res.query_id)
        assert.equal("PREPARED", res.type)
      end)
      it("should execute a prepared query", function()
        local res = assert(peer:prepare "SELECT * FROM system.local WHERE key = ?")
        local rows = assert(peer:execute(res.query_id, {"local"}, {prepared = true}))
        assert.equal("local", rows[1].key)
      end)
    end)

    describe("set_keyspace()", function()
      it("sets a peer's keyspace", function()
        local peer_k = assert(cassandra.new())
        assert(peer_k:connect())

        local res = assert(peer_k:set_keyspace "system")
        assert.equal(0, #res)
        assert.equal("SET_KEYSPACE", res.type)
        assert.equal("system", res.keyspace)

        local rows = assert(peer_k:execute "SELECT * FROM local")
        assert.equal("local", rows[1].key)
      end)
      it("connects directly in a keyspace", function()
        local peer_k = assert(cassandra.new {keyspace = "system"})
        assert(peer_k:connect())

        local rows = assert(peer_k:execute "SELECT * FROM local")
        assert.equal("local", rows[1].key)
      end)
    end)

    describe("batch()", function()
      setup(function()
        assert(peer:set_keyspace(utils.keyspace))
        assert(peer:execute [[
          CREATE TABLE IF NOT EXISTS things(
            id uuid PRIMARY KEY,
            n int
          )
        ]])
        assert(peer:execute [[
          CREATE TABLE IF NOT EXISTS counters(
            key text PRIMARY KEY,
            value counter
          )
        ]])
      end)
      teardown(function()
        assert(peer:execute "TRUNCATE things")
      end)

      after_each(function()
        assert(peer:execute "TRUNCATE counters")
      end)

      it("executes a logged batch by default", function()
        local res = assert(peer:batch {
          {"INSERT INTO things(id, n) VALUES("..uuid..", 1)"},
          {"UPDATE things SET n = 2 WHERE id = "..uuid},
          {"UPDATE things SET n = 3 WHERE id = "..uuid}
        })
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
        assert.equal(3, rows[1].n)
      end)
      it("executes a batch of queries as strings", function()
        local res = assert(peer:batch {
          "INSERT INTO things(id, n) VALUES("..uuid..", 1)",
          "UPDATE things SET n = 2 WHERE id = "..uuid,
          "UPDATE things SET n = 3 WHERE id = "..uuid
        })
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
        assert.equal(3, rows[1].n)
      end)
      it("executes batch with params", function()
        local res = assert(peer:batch({
          {"INSERT INTO things(id, n) VALUES(?, ?)", {cassandra.uuid(uuid), 1}},
          {"INSERT INTO things(id, n) VALUES(?, ?)", {cassandra.uuid(uuid), 2}},
          {"INSERT INTO things(id, n) VALUES(?, ?)", {cassandra.uuid(uuid), 3}},
        }))
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
        assert.equal(3, rows[1].n)
      end)
      it("executes an unlogged batch", function()
        local res = assert(peer:batch({
          {"INSERT INTO things(id, n) VALUES("..uuid..", 1)"},
          {"UPDATE things SET n = 2 WHERE id = "..uuid},
          {"UPDATE things SET n = 3 WHERE id = "..uuid}
        }, {logged = false}))
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
        assert.equal(3, rows[1].n)
      end)
      it("executes a counter batch", function()
        local res = assert(peer:batch({
          {"UPDATE counters SET value = value + 1 WHERE key = 'counter'"},
          {"UPDATE counters SET value = value + 1 WHERE key = 'counter'"},
          {"UPDATE counters SET value = value + 1 WHERE key = 'counter'"}
        }, {counter = true}))
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute "SELECT value FROM counters WHERE key = 'counter'")
        assert.equal(3, rows[1].value)
      end)
      it("supports protocol level timestamp", function()
        local uuid = "0d0dca5e-e1d5-11e5-89ff-93118511c17e"
        assert(peer:batch({
          {"INSERT INTO things(id, n) VALUES("..uuid..", 1)"},
          {"UPDATE things SET n = 2 WHERE id = "..uuid},
          {"UPDATE things SET n = 3 WHERE id = "..uuid}
        }, {timestamp = 1428311323417123}))

        local rows = assert(peer:execute("SELECT n,writetime(n) FROM things WHERE id = "..uuid))
        assert.equal(3, rows[1].n)
        assert.equal(1428311323417123, rows[1]["writetime(n)"])
      end)
      it("supports serial consistency", function()
        assert(peer:batch({
          {"INSERT INTO things(id, n) VALUES("..uuid..", 1)"},
          {"UPDATE things SET n = 2 WHERE id = "..uuid},
          {"UPDATE things SET n = 3 WHERE id = "..uuid}
        }, {serial_consistency = cassandra.consistencies.local_serial}))

        local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
        assert.equal(3, rows[1].n)
      end)
      it("execute prepared queries", function()
        local res1 = assert(peer:prepare "INSERT INTO things(id,n) VALUES(?,?)")
        local res2 = assert(peer:prepare "UPDATE things set n = ? WHERE id = ?")

        local q1, q2 = res1.query_id, res2.query_id

        local res = assert(peer:batch({
          {q1, {cassandra.uuid(uuid), 1}},
          {q2, {2, cassandra.uuid(uuid)}},
          {q2, {3, cassandra.uuid(uuid)}},
          {q2, {4, cassandra.uuid(uuid)}},
          {q2, {5, cassandra.uuid(uuid)}}
        }, {prepared = true}))
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
        assert.equal(5, rows[1].n)
      end)
      it("execute prepared queries without args", function()
        local res1 = assert(peer:prepare("INSERT INTO things(id,n) VALUES("..uuid..",1)"))
        local res2 = assert(peer:prepare("UPDATE things set n = 2 WHERE id = "..uuid))

        local q1, q2 = res1.query_id, res2.query_id

        local res = assert(peer:batch({
          q1, q2
        }, {prepared = true}))
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
        assert.equal(2, rows[1].n)
      end)
      it("returns CQL errors", function()
        local res, err, code = peer:batch {
          {"INSERT FOO"}, {"INSERT BAR"}
        }
        assert.is_nil(res)
        assert.equal("[Syntax error] line 0:-1 mismatched input '<EOF>' expecting '('", err)
        assert.equal(cassandra.cql_errors.SYNTAX_ERROR, code)
      end)
    end) -- batch()

    describe("Types", function()
      setup(function()
        assert(peer:set_keyspace(utils.keyspace))
        assert(peer:execute [[
          CREATE TYPE IF NOT EXISTS address(
            street text,
            city text,
            zip int,
            country text
          )
        ]])
        assert(peer:execute [[
          CREATE TABLE IF NOT EXISTS cql_types(
            id uuid PRIMARY KEY,
            ascii_sample ascii,
            bigint_sample bigint,
            blob_sample blob,
            boolean_sample boolean,
            double_sample double,
            float_sample float,
            int_sample int,
            text_sample text,
            timestamp_sample timestamp,
            varchar_sample varchar,
            varint_sample varint,
            timeuuid_sample timeuuid,
            inet_sample inet,
            list_sample_text list<text>,
            list_sample_int list<int>,
            map_sample_text_text map<text, text>,
            map_sample_text_int map<text, int>,
            set_sample_text set<text>,
            set_sample_int set<int>,
            udt_sample frozen<address>,
            tuple_sample tuple<text, text>
          )
        ]])
      end)

      for fixture_type, fixture_values in pairs(utils.cql_fixtures) do
        it("["..fixture_type.."] encoding/decoding", function()
          local insert_query = string.format("INSERT INTO cql_types(id, %s_sample) VALUES(?, ?)", fixture_type)
          local select_query = string.format("SELECT %s_sample FROM cql_types WHERE id = ?", fixture_type)

          for _, fixture in ipairs(fixture_values) do
            local res = assert(peer:execute(insert_query, {cassandra.uuid(uuid), cassandra[fixture_type](fixture)}))
            assert.equal("VOID", res.type)

            local rows = assert(peer:execute(select_query, {cassandra.uuid(uuid)}))
            assert.equal(1, #rows)

            local decoded = rows[1][fixture_type.."_sample"]
            assert.not_nil(decoded)
            assert.fixture(fixture_type, fixture, decoded)
          end
        end)
      end

      it("[unset] (NULL)", function()
        assert.is_table(cassandra.unset)
        assert.equal("unset", cassandra.unset.type_id)

        local rows = assert(peer:execute("SELECT * FROM cql_types WHERE id = "..uuid))
        assert.equal(1, #rows)
        assert.is_string(rows[1].ascii_sample)

        local res = assert(peer:execute("UPDATE cql_types SET ascii_sample = ? WHERE id = ?", {cassandra.unset, cassandra.uuid(uuid)}))
        assert.equal("VOID", res.type)

        rows = assert(peer:execute("SELECT * FROM cql_types WHERE id = "..uuid))
        assert.equal(1, #rows)
        assert.is_nil(rows[1].ascii_sample)
      end)
      it("[list<type>] encoding/decoding", function()
        for _, fixture in ipairs(utils.cql_map_fixtures) do
          local insert_query = string.format("INSERT INTO cql_types(id, map_sample_%s_%s) VALUES(?, ?)", fixture.key_type_name, fixture.value_type_name)
          local select_query = string.format("SELECT map_sample_%s_%s FROM cql_types WHERE id = ?", fixture.key_type_name, fixture.value_type_name)

          local res = assert(peer:execute(insert_query, {cassandra.uuid(uuid), cassandra.map(fixture.value)}))
          assert.equal("VOID", res.type)

          local rows = assert(peer:execute(select_query, {cassandra.uuid(uuid)}))
          assert.equal(1, #rows)

          local decoded = rows[1]["map_sample_"..fixture.key_type_name.."_"..fixture.value_type_name]
          assert.not_nil(decoded)
          assert.fixture("list", fixture.value, decoded)
        end
      end)
      it("[map<type, types>] encoding/decoding empty table", function()
        local insert_query = "INSERT INTO cql_types(id, map_sample_text_int) VALUES(?, ?)"
        local select_query = "SELECT * FROM cql_types WHERE id = ?"
        local fixture = {}

        local res = assert(peer:execute(insert_query, {cassandra.uuid(uuid), cassandra.map(fixture)}))
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute(select_query, {cassandra.uuid(uuid)}))
        assert.equal(1, #rows)
        assert.is_nil(rows[1].map_sample_text_int)
      end)
      it("[list<type, type>] encoding/decoding", function()
        for _, fixture in ipairs(utils.cql_list_fixtures) do
          local insert_query = string.format("INSERT INTO cql_types(id, list_sample_%s) VALUES(?, ?)", fixture.type_name)
          local select_query = string.format("SELECT list_sample_%s FROM cql_types WHERE id = ?", fixture.type_name)

          local res = assert(peer:execute(insert_query, {cassandra.uuid(uuid), cassandra.list(fixture.value)}))
          assert.equal("VOID", res.type)

          local rows = assert(peer:execute(select_query, {cassandra.uuid(uuid)}))
          assert.equal(1, #rows)

          local decoded = rows[1]["list_sample_"..fixture.type_name]
          assert.not_nil(decoded)
          assert.fixture("list", fixture.value, decoded)
        end
      end)
      it("[set<type>] encoding/decoding", function()
        for _, fixture in ipairs(utils.cql_list_fixtures) do
          local insert_query = string.format("INSERT INTO cql_types(id, set_sample_%s) VALUES(?, ?)", fixture.type_name)
          local select_query = string.format("SELECT set_sample_%s FROM cql_types WHERE id = ?", fixture.type_name)

          local res = assert(peer:execute(insert_query, {cassandra.uuid(uuid), cassandra.set(fixture.value)}))
          assert.equal("VOID", res.type)

          local rows = assert(peer:execute(select_query, {cassandra.uuid(uuid)}))
          assert.equal(1, #rows)

          local decoded = rows[1]["set_sample_"..fixture.type_name]
          assert.not_nil(decoded)
          assert.same_set(fixture.value, decoded)
        end
      end)
      it("[udt] encoding/decoding", function()
        local res = assert(peer:execute("INSERT INTO cql_types(id, udt_sample) VALUES(?, ?)", {
          cassandra.uuid(uuid),
          cassandra.udt {"montgomery st", "san francisco", 94111, nil} -- nil country
        }))
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute("SELECT udt_sample FROM cql_types WHERE id = ?", {cassandra.uuid(uuid)}))
        assert.equal(1, #rows)
        assert.same({
          street = "montgomery st",
          city = "san francisco",
          zip = 94111,
          country = ""
        }, rows[1].udt_sample)
      end)
      it("[tuple] encoding/decoding", function()
        for _, fixture in ipairs(utils.cql_tuple_fixtures) do
          local res = assert(peer:execute("INSERT INTO cql_types(id, tuple_sample) VALUES(?, ?)", {
            cassandra.uuid(uuid),
            cassandra.tuple(fixture.value)
          }))
          assert.equal("VOID", res.type)

          local rows = assert(peer:execute("SELECT tuple_sample FROM cql_types WHERE id = ?", {cassandra.uuid(uuid)}))
          assert.equal(1, #rows)

          local tuple = rows[1].tuple_sample
          assert.not_nil(tuple)
          assert.equal(fixture.value[1], tuple[1])
          assert.equal(fixture.value[2], tuple[2])
        end
      end)
    end)

    describe("type inference", function()
      for _, fixture_type in ipairs({"ascii", "boolean", "float", "int", "text", "varchar"}) do
        local fixture_values = utils.cql_fixtures[fixture_type]
        it("["..fixture_type.."] is inferred", function()
          for _, fixture in ipairs(fixture_values) do
            local insert_query = string.format("INSERT INTO cql_types(id, %s_sample) VALUES(?, ?)", fixture_type)
            local select_query = string.format("SELECT %s_sample FROM cql_types WHERE id = ?", fixture_type)

            local res = assert(peer:execute(insert_query, {cassandra.uuid(uuid), fixture}))
            assert.equal("VOID", res.type)

            local rows = assert(peer:execute(select_query, {cassandra.uuid(uuid)}))
            assert.equal(1, #rows)

            local decoded = rows[1][fixture_type.."_sample"]
            assert.not_nil(decoded)
            assert.fixture(fixture_type, fixture, decoded)
          end
        end)
      end
      it("[map<type, type>] is inferred", function()
        for _, fixture in ipairs(utils.cql_list_fixtures) do
          local insert_query = string.format("INSERT INTO cql_types(id, list_sample_%s) VALUES(?, ?)", fixture.type_name)
          local select_query = string.format("SELECT list_sample_%s FROM cql_types WHERE id = ?", fixture.type_name)

          local res = assert(peer:execute(insert_query, {cassandra.uuid(uuid), fixture.value}))
          assert.equal("VOID", res.type)

          local rows = assert(peer:execute(select_query, {cassandra.uuid(uuid)}))
          assert.equal(1, #rows)

          local decoded = rows[1]["list_sample_"..fixture.type_name]
          assert.not_nil(decoded)
          assert.fixture("list", fixture.value, decoded)
        end
      end)
    end)
    it("[set<type>] is inferred", function()
      for _, fixture in ipairs(utils.cql_list_fixtures) do
        local insert_query = string.format("INSERT INTO cql_types(id, set_sample_%s) VALUES(?, ?)", fixture.type_name)
        local select_query = string.format("SELECT set_sample_%s FROM cql_types WHERE id = ?", fixture.type_name)

        local res = assert(peer:execute(insert_query, {cassandra.uuid(uuid), fixture.value}))
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute(select_query, {cassandra.uuid(uuid)}))
        assert.equal(1, #rows)

        local decoded = rows[1]["set_sample_"..fixture.type_name]
        assert.not_nil(decoded)
        assert.same_set(fixture.value, decoded)
      end
    end)

    describe("pagination", function()
      local n_inserts = 1001
      local n_select = 20
      setup(function()
        assert(peer:execute [[
          CREATE TABLE IF NOT EXISTS metrics(
            id int PRIMARY KEY,
            n int
          )
        ]])
        assert(peer:execute "TRUNCATE metrics")
        for i = 1, n_inserts do
          assert(peer:execute("INSERT INTO metrics(id,n) VALUES(?,?)", {i, i*i}))
        end
      end)

      it("default page size", function()
        local rows = assert(peer:execute "SELECT * FROM metrics")
        assert.equal(1000, #rows)
      end)
      it("page_size option", function()
        local rows = assert(peer:execute("SELECT * FROM metrics", nil, {page_size = n_select}))
        assert.equal(n_select, #rows)
      end)
      it("has_more_pages flag", function()
        local rows = assert(peer:execute("SELECT * FROM metrics", nil, {page_size = n_select}))
        assert.True(rows.meta.has_more_pages)
      end)
      it("paging_state", function()
        local rows1 = assert(peer:execute("SELECT * FROM metrics", nil, {page_size = n_select}))
        assert.truthy(rows1.meta.paging_state)
        local rows2 = assert(peer:execute("SELECT * FROM metrics", nil, {
          page_size = n_select,
          paging_state = rows1.meta.paging_state
        }))
        assert.equal(n_select, #rows2)
        assert.not_same(rows1, rows2)
      end)
      describe("iterate()", function()
        it("iterates", function()
          local n_page = 0
          local opts, buf = {page_size = n_select}, {}
          for rows, err, page in peer:iterate("SELECT * FROM metrics", nil, opts) do
            assert.is_nil(err)
            assert.is_number(page)
            assert.is_table(rows)
            assert.equal("ROWS", rows.type)
            n_page = n_page + 1
            for _, v in ipairs(rows) do buf[#buf+1] = v end
          end

          assert.equal(n_inserts, #buf)
          assert.equal(math.ceil(n_inserts/n_select), n_page)
        end)
        it("returns 1st page at once", function()
          local n = 0
          local opts = {page_size = n_inserts}
          for rows, err, page in peer:iterate("SELECT * FROM metrics", nil, opts) do
            assert.is_nil(err)
            assert.equal(1, page)
            assert.equal(n_inserts, #rows)
            n = n + 1
          end
          assert.equal(1, n)
        end)
        it("reports errors", function()
          -- additional iteration to report error
          local opts = {page_size = n_select}
          for rows, err, page in peer:iterate("SELECT * FROM metrics WHERE col = 'a'", nil, opts) do
            assert.equal("[Invalid] Undefined name col in where clause ('col = 'a'')", err)
            assert.equal(0, page)
            assert.same({meta = {has_more_pages = false}}, rows)
          end
        end)
        it("executes prepared statements", function()
          local q = assert(peer:prepare "SELECT * FROM METRICS")

          local n_page = 0
          local opts = {page_size = n_select, prepared = true}
          for rows, err, page in peer:iterate(q.query_id, nil, opts) do
            assert.is_nil(err)
            assert.is_number(page)
            assert.is_table(rows)
            assert.equal("ROWS", rows.type)
            n_page = n_page + 1
          end

          assert.equal(math.ceil(n_inserts/n_select), n_page)
        end)
      end)
    end)
  end) -- CQL
end)
