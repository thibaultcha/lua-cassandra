local helpers = require "spec.helpers"
local cassandra = require "cassandra"

describe("cassandra (host)", function()
  setup(function()
    helpers.ccm_start()
  end)

  it("exposes version field", function()
    assert.matches("%d%.%d%.%d", cassandra._VERSION)
  end)

  describe("consistencies", function()
    it("exposes Cassandra data consistencies", function()
      assert.is_table(cassandra.consistencies)

      local cql = require "cassandra.cql"
      for t in pairs(cql.consistencies) do
        assert.truthy(cassandra.consistencies[t])
      end
    end)
  end)
  describe("cql_errors", function()
    it("exposes Cassandra CQL errors", function()
      assert.is_table(cassandra.cql_errors)

      local cql = require "cassandra.cql"
      for t in pairs(cql.errors) do
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
      assert.has_error(cassandra.uuid, "bad argument #1 to 'uuid()' (got nil)")
      assert.has_error(cassandra.map, "bad argument #1 to 'map()' (got nil)")
      assert.has_error(cassandra.list, "bad argument #1 to 'list()' (got nil)")
      assert.has_error(cassandra.timestamp, "bad argument #1 to 'timestamp()' (got nil)")
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
    it("3rd return value indicates potential down host", function()
      local peer = assert(cassandra.new {
        host = "255.255.255.254"
      })
      peer:settimeout(1000)
      local ok, err, maybe_down = peer:connect()
      assert.is_nil(ok)
      assert.equal("timeout", err)
      assert.True(maybe_down)
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
      assert(helpers.create_keyspace(p, helpers.keyspace))
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
      describe("protocol v3 options", function()
        setup(function()
          assert(peer:set_keyspace(helpers.keyspace))
          assert(peer:execute [[
            CREATE TABLE IF NOT EXISTS options(
              id int PRIMARY KEY,
              n int
            )
          ]])
        end)
        teardown(function()
          assert(peer:execute "TRUNCATE options")
        end)

        it("supports protocol level timestamp", function()
          assert(peer:execute("INSERT INTO options(id,n) VALUES(1, 10)", nil, {
            timestamp = 1428311323417123
          }))

          local rows = assert(peer:execute "SELECT n,writetime(n) FROM options WHERE id = 1")
          assert.equal(10, rows[1].n)
          assert.equal(1428311323417123, rows[1]["writetime(n)"])
        end)
        it("supports serial consistency", function()
          assert(peer:execute("INSERT INTO options(id, n) VALUES(2, 20) IF NOT EXISTS", nil, {
            serial_consistency = cassandra.consistencies.local_serial
          }))

          local rows = assert(peer:execute "SELECT * FROM options WHERE id = 2")
          assert.equal(1, #rows)
          assert.equal(20, rows[1].n)
        end)
        it("supports named parameters", function()
          assert(peer:execute("INSERT INTO options(id, n) VALUES(?, ?)", {
            id = 3,
            n = 30
          }, {
            named = true
          }))

          local rows = assert(peer:execute "SELECT * FROM options WHERE id = 3")
          assert.equal(1, #rows)
          assert.equal(30, rows[1].n)
        end)
      end)
      describe("tracing", function()
        it("appends tracing_id field to result", function()
          local res = assert(peer:execute("INSERT INTO options(id,n) VALUES(4, 10)", nil, {
            tracing = true
          }))

          assert.is_string(res.tracing_id)
        end)
        describe("get_trace()", function()
          it('retrieves a tracing session and events', function()
            local res = assert(peer:execute("INSERT INTO options(id,n) VALUES(5, 10)", nil, {
              tracing = true
            }))

            local trace, err, timeout
            local tstart = os.time()
            repeat
              trace, err = peer:get_trace(res.tracing_id)
              if not trace and not string.find(err, "no trace with id", nil, true) then
                error(err)
              end
              timeout = os.time() - tstart >= 5
            until trace or timeout

            if timeout then
              error("timed out while waiting for trace")
            end

            local trace = assert(peer:get_trace(res.tracing_id))
            assert.equal("127.0.0.1", trace.client)
            assert.equal("QUERY", trace.command)
            assert.is_table(trace.events)
            assert.True(#trace.events > 0)
            assert.is_table(trace.parameters)
          end)
        end)
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
        assert(peer:set_keyspace(helpers.keyspace))
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
      --[[
      not supported anymore for now
      pending("executes a batch of queries as strings", function()
        local res = assert(peer:batch {
          "INSERT INTO things(id, n) VALUES("..uuid..", 1)",
          "UPDATE things SET n = 2 WHERE id = "..uuid,
          "UPDATE things SET n = 3 WHERE id = "..uuid
        })
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
        assert.equal(3, rows[1].n)
      end)
      --]]
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
      it("executes prepared queries", function()
        local res1 = assert(peer:prepare "INSERT INTO things(id,n) VALUES(?,?)")
        local res2 = assert(peer:prepare "UPDATE things set n = ? WHERE id = ?")

        local q1, q2 = res1.query_id, res2.query_id

        local res = assert(peer:batch({
          {[2] = {cassandra.uuid(uuid), 1}, [3] = q1},
          {[2] = {2, cassandra.uuid(uuid)}, [3] = q2},
          {[2] = {3, cassandra.uuid(uuid)}, [3] = q2},
          {[2] = {4, cassandra.uuid(uuid)}, [3] = q2},
          {[2] = {5, cassandra.uuid(uuid)}, [3] = q2}
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
          {[3] = q1}, {[3] = q2}
        }, {prepared = true}))
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
        assert.equal(2, rows[1].n)
      end)
      it("returns CQL errors", function()
        local res, err, code = peer:batch {
          {"INSERT INTO things(id,n) VALUES()"}, {"INSERT BAR"}
        }
        assert.is_nil(res)
        assert.equal("[Syntax error] line 1:32 no viable alternative at input ')' (... things(id,n) VALUES([)])", err)
        assert.equal(cassandra.cql_errors.SYNTAX_ERROR, code)
      end)
      describe("protocol v3 options", function()
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
            {"INSERT INTO things(id, n) VALUES("..uuid..", 1) IF NOT EXISTS"},
            {"UPDATE things SET n = 2 WHERE id = "..uuid},
            {"UPDATE things SET n = 3 WHERE id = "..uuid}
          }, {serial_consistency = cassandra.consistencies.local_serial}))

          local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
          assert.equal(1, #rows)
        end)
        --[[
        not supported because of CQL issue we reported:
        https://issues.apache.org/jira/browse/CASSANDRA-10246
        pending("supports named parameters", function()
          assert(peer:batch({
            {"INSERT INTO things(id, n) VALUES("..uuid..", 1)"},
            {"UPDATE things SET n = ? WHERE id = ?", {n = 20, id = cassandra.uuid(uuid)}},
            {"UPDATE things SET n = ? WHERE id = ?", {n = 21, id = cassandra.uuid(uuid)}}
          }, {named = true}))

          local rows = assert(peer:execute("SELECT * FROM things WHERE id = "..uuid))
          assert.equal(1, #rows)
          assert.equal(3, rows[1].n)
        end)
        ]]
      end)
    end) -- batch()

    describe("Types marshalling", function()
      setup(function()
        assert(peer:set_keyspace(helpers.keyspace))
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
            id int PRIMARY KEY,
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
            uuid_sample uuid,
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

      local _id = 1
      local fmt = string.format

      for cql_t_name, fixture_values in pairs(helpers.cql_fixtures) do
        local col_name = cql_t_name.."_sample"
        local insert_q = fmt("INSERT INTO cql_types(id, %s) VALUES(?, ?)", col_name)
        local select_q = fmt("SELECT %s FROM cql_types WHERE id = ?", col_name)

        it("["..cql_t_name.."]", function()
          for i = 1, #fixture_values do
            local fixture = fixture_values[i]
            local res = assert(peer:execute(insert_q, {
              _id,
              cassandra[cql_t_name](fixture)
            }))
            assert.equal("VOID", res.type)

            local rows = assert(peer:execute(select_q, {_id}))
            assert.equal(1, #rows)

            local decoded = rows[1][col_name]
            assert.fixture(cql_t_name, fixture, decoded)
          end
        end)
      end

      it("[unset]", function()
        assert.is_table(cassandra.unset)

        local rows = assert(peer:execute("SELECT * FROM cql_types WHERE id = ".._id))
        assert.equal(1, #rows)
        assert.is_string(rows[1].ascii_sample)

        local res = assert(peer:execute("UPDATE cql_types SET ascii_sample = ? WHERE id = ?", {
          cassandra.unset,
          _id
        }))
        assert.equal("VOID", res.type)

        rows = assert(peer:execute("SELECT * FROM cql_types WHERE id = ".._id))
        assert.equal(1, #rows)
        assert.is_nil(rows[1].ascii_sample)
      end)
      it("[map<type, type>]", function()
        for _, fixture in ipairs(helpers.cql_map_fixtures) do
          local insert_q = fmt("INSERT INTO cql_types(id, %s) VALUES(?, ?)", fixture.name)
          local select_q = fmt("SELECT %s FROM cql_types WHERE id = ?", fixture.name)

          local res = assert(peer:execute(insert_q, {
            _id,
            cassandra.map(fixture.val)
          }))
          assert.equal("VOID", res.type)

          local rows = assert(peer:execute(select_q, {_id}))
          assert.equal(1, #rows)

          local decoded = rows[1][fixture.name]
          assert.same(fixture.val, decoded)
        end
      end)
      it("[list<type>]", function()
        for _, fixture in ipairs(helpers.cql_list_fixtures) do
          local insert_q = fmt("INSERT INTO cql_types(id, %s) VALUES(?, ?)", fixture.name)
          local select_q = fmt("SELECT %s FROM cql_types WHERE id = ?", fixture.name)

          local res = assert(peer:execute(insert_q, {
            _id,
            cassandra.list(fixture.val)
          }))
          assert.equal("VOID", res.type)

          local rows = assert(peer:execute(select_q, {_id}))
          assert.equal(1, #rows)

          local decoded = rows[1][fixture.name]
          assert.same(fixture.val, decoded)
        end
      end)
      it("[set<type>]", function()
        for _, fixture in ipairs(helpers.cql_set_fixtures) do
          local insert_q = fmt("INSERT INTO cql_types(id, %s) VALUES(?, ?)", fixture.name)
          local select_q = fmt("SELECT %s FROM cql_types WHERE id = ?", fixture.name)

          local res = assert(peer:execute(insert_q, {
            _id,
            cassandra.set(fixture.val)
          }))
          assert.equal("VOID", res.type)

          local rows = assert(peer:execute(select_q, {_id}))
          assert.equal(1, #rows)

          local decoded = rows[1][fixture.name]
          assert.same_set(fixture.val, decoded)
        end
      end)
      it("[udt]", function()
        local res = assert(peer:execute("INSERT INTO cql_types(id, udt_sample) VALUES(?, ?)", {
          _id,
          cassandra.udt {"montgomery st", "san francisco", 94111, nil} -- nil country
        }))
        assert.equal("VOID", res.type)

        local rows = assert(peer:execute("SELECT udt_sample FROM cql_types WHERE id = ?", {
          _id
        }))
        assert.equal(1, #rows)
        assert.same({
          street = "montgomery st",
          city = "san francisco",
          zip = 94111,
          country = ""
        }, rows[1].udt_sample)
      end)
      it("[tuple]", function()
        for _, fixture in ipairs(helpers.cql_tuple_fixtures) do
          local res = assert(peer:execute("INSERT INTO cql_types(id, tuple_sample) VALUES(?, ?)", {
            _id,
            cassandra.tuple(fixture.val)
          }))
          assert.equal("VOID", res.type)

          local rows = assert(peer:execute("SELECT tuple_sample FROM cql_types WHERE id = ?", {
            _id
          }))
          assert.equal(1, #rows)

          local tuple = rows[1].tuple_sample
          assert.not_nil(tuple)
          assert.equal(fixture.val[1], tuple[1])
          assert.equal(fixture.val[2], tuple[2])
        end
      end)
      describe("inferences", function()
        local infered = {"ascii", "boolean", "float", "int", "text", "varchar"}
        for i = 1, #infered do
          local fixture_type = infered[i]
          local col_name = fixture_type.."_sample"
          local fixtures = helpers.cql_fixtures[fixture_type]
          local insert_q = fmt("INSERT INTO cql_types(id, %s) VALUES(?, ?)", col_name)
          local select_q = fmt("SELECT %s FROM cql_types WHERE id = ?", col_name)

          it("["..fixture_type.."] is inferred", function()
            for _, fixture in ipairs(fixtures) do
              local res = assert(peer:execute(insert_q, {_id, fixture}))
              assert.equal("VOID", res.type)

              local rows = assert(peer:execute(select_q, {_id}))
              assert.equal(1, #rows)

              local decoded = rows[1][fixture_type.."_sample"]
              assert.fixture(fixture_type, fixture, decoded)
            end
          end)
        end
        it("[map<type, type>] is inferred", function()
          for _, fixture in ipairs(helpers.cql_map_fixtures) do
            local insert_q = fmt("INSERT INTO cql_types(id, %s) VALUES(?, ?)", fixture.name)
            local select_q = fmt("SELECT %s FROM cql_types WHERE id = ?", fixture.name)

            local res = assert(peer:execute(insert_q, {_id, fixture.val}))
            assert.equal("VOID", res.type)

            local rows = assert(peer:execute(select_q, {_id}))
            assert.equal(1, #rows)

            local decoded = rows[1][fixture.name]
            assert.same(fixture.val, decoded)
          end
        end)
      end)
      it("[list<type>] is inferred", function()
        for _, fixture in ipairs(helpers.cql_list_fixtures) do
          local insert_q = fmt("INSERT INTO cql_types(id, %s) VALUES(?, ?)", fixture.name)
          local select_q = fmt("SELECT %s FROM cql_types WHERE id = ?", fixture.name)

          local res = assert(peer:execute(insert_q, {
            _id,
            fixture.val
          }))
          assert.equal("VOID", res.type)

          local rows = assert(peer:execute(select_q, {_id}))
          assert.equal(1, #rows)

          local decoded = rows[1][fixture.name]
          assert.same(fixture.val, decoded)
        end
      end)
    end) -- Types

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
