local cql = require "cassandra.cql2"
local helpers = require "spec.helpers"
local cassandra = require "cassandra"
local Buffer = cql.buffer

describe("is_list()", function()
  it("detects lists", function()
    assert.True(cql.is_list { "a", "b", "c", "d" })
    assert.False(cql.is_list { ["1"] = "a", ["2"] = "b", ["3"] = "c", ["4"] = "d" })
    assert.False(cql.is_list { "a", "b", "c", foo = "d" })
    assert.False(cql.is_list())
    assert.False(cql.is_list(false))
    assert.False(cql.is_list(true))
    assert.False(cql.is_list(""))
  end)
end)

describe("CQL module", function()

    it("has the necessary requests and frame_reader modules", function()
        assert.is_table(cql.requests)
        assert.is_table(cql.frame_reader)
    end)

    it("has the necessary constants", function()
        assert.is_table(cql.CONSISTENCIES)
        assert.is_table(cql.ERRORS)
        assert.is_table(cql.TYP_UNSET)
        assert.is_table(cql.TYP_NULL)
    end)
end)

for protocol_version = 2, 3 do
  describe("CQL marshalling v" .. protocol_version, function()

    for cql_t_name, fixtures in pairs(helpers.cql_fixtures) do
      local cql_t = cql.types[cql_t_name]
      local marshaller = cassandra[cql_t_name]

      it("[" .. cql_t_name .. "]", function()
        for i = 1, #fixtures do
          local fixture = fixtures[i]

          local buf_w = Buffer.new_w(protocol_version)
          Buffer.write_cql_value(buf_w, marshaller(fixture))

          local buf_r = Buffer.new_r(protocol_version, Buffer.get(buf_w))
          assert.fixture(cql_t_name, fixture, Buffer.read_cql_value(buf_r, { cql_type = cql_t }))
        end
      end)
    end

    it("[list<T>]", function()
      local fixtures = helpers.cql_list_fixtures

      for i = 1, #fixtures do
        local fixture = fixtures[i]

        local buf_w = Buffer.new_w(protocol_version)
        Buffer.write_cql_value(buf_w, fixture)

        local buf_r = Buffer.new_r(protocol_version, Buffer.get(buf_w))
        assert.same(fixture.value, Buffer.read_cql_value(buf_r, fixture))
      end
    end)

    it("[set<T>]", function()
      local fixtures = helpers.cql_set_fixtures

      for i = 1, #fixtures do
        local fixture = fixtures[i]

        local buf_w = Buffer.new_w(protocol_version)
        Buffer.write_cql_value(buf_w, fixture)

        local buf_r = Buffer.new_r(protocol_version, Buffer.get(buf_w))
        assert.same(fixture.value, Buffer.read_cql_value(buf_r, fixture))
      end
    end)

    it("[map<T, T>]", function()
      local fixtures = helpers.cql_map_fixtures

      for i = 1, #fixtures do
        local fixture = fixtures[i]

        local buf_w = Buffer.new_w(protocol_version)
        Buffer.write_cql_value(buf_w, fixture)

        local buf_r = Buffer.new_r(protocol_version, Buffer.get(buf_w))
        assert.same(fixture.value, Buffer.read_cql_value(buf_r, fixture))
      end
    end)

    it("[tuple<T, T>]", function()
      local fixtures = helpers.cql_tuple_fixtures

      for i = 1, #fixtures do
        local fixture = fixtures[i]

        local buf_w = Buffer.new_w(protocol_version)
        Buffer.write_cql_value(buf_w, fixture)

        local buf_r = Buffer.new_r(protocol_version, Buffer.get(buf_w))
        assert.same(fixture.value, Buffer.read_cql_value(buf_r, fixture))
      end
    end)

    it("[udt]", function()
      local fixtures = helpers.cql_udt_fixtures

      for i = 1, #fixtures do
        local fixture = fixtures[i]

        local buf_w = Buffer.new_w(protocol_version)
        Buffer.write_cql_value(buf_w, fixture)

        local buf_r = Buffer.new_r(protocol_version, Buffer.get(buf_w))
        assert.same(fixture.read, Buffer.read_cql_value(buf_r, fixture))
      end
    end)

    describe("notations", function()
      it("[string list]", function()
        local list = { "hello", "world", "goodbye" }

        local buf_w = Buffer.new_w(protocol_version)
        Buffer.write_string_list(buf_w, list)

        local buf_r = Buffer.new_r(protocol_version, Buffer.get(buf_w))
        assert.same(list, Buffer.read_string_list(buf_r))
      end)

      it("[string multimap]", function()
        local multimap = {
          hello = { "world", "universe" },
          goodbye = { "universe", "world" },
          foo = { "bar" }
        }

        local buf_w = Buffer.new_w(protocol_version)
        Buffer.write_string_multimap(buf_w, multimap)

        local buf_r = Buffer.new_r(protocol_version, Buffer.get(buf_w))
        assert.same(multimap, Buffer.read_string_multimap(buf_r))
      end)
    end)

    describe("write_cql_values()", function()
      it("writes given values and infer their types", function()
        local values = {
          true,
          42,
          "hello world",
          { "hello", "world" },
          { hello = "world" }
        }

        local buf_w = Buffer.new_w(protocol_version)
        Buffer.write_short(buf_w, #values)

        for i = 1, #values do
            Buffer.write_cql_value(buf_w, values[i])
        end

        local buf_r = Buffer.new_r(protocol_version, Buffer.get(buf_w))

        assert.equal(#values, Buffer.read_short(buf_r))
        assert.True(Buffer.read_cql_value(buf_r, { cql_type = cql.types.boolean }))
        assert.equal(values[2], Buffer.read_cql_value(buf_r, { cql_type = cql.types.int }))
        assert.equal(values[3], Buffer.read_cql_value(buf_r, { cql_type = cql.types.text }))
        assert.same(values[4], Buffer.read_cql_value(buf_r, {
            cql_type = cql.types.set,
            cql_type_value = {
                cql_type = cql.types.text
            }
        }))
        assert.same(values[5], Buffer.read_cql_value(buf_r, {
            cql_type = cql.types.map,
            cql_type_value = {
                { cql_type = cql.types.text },
                { cql_type = cql.types.text },
            }
        }))
      end)
    end)
  end)

  describe("CQL requests", function()
    local requests = cql.requests

    it("is a table with a retries field", function()
      local r = requests.query("SELECT * FROM peers")
      assert.equal(0, r.retries)
    end)

    describe("build_frame()", function()

      it("writes the frame's body", function()
        local r = requests.query("SELECT * FROM peers")
        local frame = requests.build_frame(r, protocol_version)
        assert.is_string(frame)
      end)

      it("rebuilds the frame if called multiple times", function()
        local r = requests.query("SELECT * FROM local")
        local frame1 = requests.build_frame(r, protocol_version)
        local frame2 = requests.build_frame(r, protocol_version)
        assert.equal(frame1, frame2)
        assert.matches("SELECT * FROM local", frame1, nil, true)

        r.query = "SELECT key FROM local"

        local frame3 = requests.build_frame(r, protocol_version)
        assert.matches("SELECT key FROM local", frame3, nil, true)
      end)
    end)

    describe("execute_prepared", function()

      it("has a 'query' attribute to allow re-preparing", function()
        local query = "SELECT * FROM system.peers"
        local args = { 123, "hello" }
        local opts = {
          page_size = 10,
          consistency = cassandra.consistencies.one
        }

        local r = requests.execute_prepared("1234", args, opts)
        requests.build_frame(r, protocol_version)
        assert.equal("1234", r.query_id)
        assert.same(args, r.args)
        assert.same(opts, r.opts)
        assert.is_nil(r.query)

        r = requests.execute_prepared("1234", args, opts, query)
        assert.equal(query, r.query)
      end)
    end)

    describe("batch", function()

      it("has a type attribute set from options", function()
        local r = requests.batch({}, {})
        assert.equal(1, r.type) -- unlogged

        r = requests.batch({}, { logged = false })
        assert.equal(1, r.type) -- unlogged

        r = requests.batch({}, { logged = true })
        assert.equal(0, r.type) -- logged

        r = requests.batch({}, { counter = true })
        assert.equal(2, r.type) -- counter
      end)

      it("has a 'queries' attribute to allow re-preparing", function()
        local opts = {
          counter = true,
          prepared = true,
          consistency = cassandra.consistencies.ONE
        }

        assert.is_number(opts.consistency)

        local queries = {
          {"UPDATE users SET value = value + 1 WHERE key = 'batch'", nil, "1234"},
          {"UPDATE users SET value = value + 1 WHERE key = ?", {'batch'}, "5678"},
        }

        local r = requests.batch(queries, opts)
        requests.build_frame(r, protocol_version)
        assert.same(opts, r.opts)
        assert.same(queries, r.queries) -- no modifications
      end)
    end)
  end)
end
