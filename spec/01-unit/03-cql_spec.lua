local helpers = require "spec.helpers"
local cassandra = require "cassandra"
local cql = require "cassandra.cql"
local Buffer = cql.buffer

describe("is_array()", function()
  it("detects arrays", function()
    assert.True(cql.is_array {"a", "b", "c", "d"})
    assert.False(cql.is_array {["1"] = "a", ["2"] = "b", ["3"] = "c", ["4"] = "d"})
    assert.False(cql.is_array {"a", "b", "c", foo = "d"})
    assert.False(cql.is_array())
    assert.False(cql.is_array(false))
    assert.False(cql.is_array(true))
    assert.False(cql.is_array "")
  end)
end)

for protocol_version = 2, 3 do
  describe("CQL marshalling v"..protocol_version, function()
    for cql_t_name, fixtures in pairs(helpers.cql_fixtures) do
      local cql_t = cql.types[cql_t_name]
      local marshaller = cassandra[cql_t_name]

      it("["..cql_t_name.."]", function()
        for i = 1, #fixtures do
          local fixture = fixtures[i]
          local buf, decoded

          buf = Buffer.new(protocol_version)
          buf:write_cql_value(marshaller(fixture))
          buf:reset()
          decoded = buf:read_cql_value({__cql_type = cql_t})
          assert.fixture(cql_t_name, fixture, decoded)
        end
      end)
    end

    it("[list<T>]", function()
      local fixtures = helpers.cql_list_fixtures
      for i = 1, #fixtures do
        local fixture = fixtures[i]
        local buf, decoded

        buf = Buffer.new(protocol_version)
        buf:write_cql_value(fixture)
        buf:reset()
        decoded = buf:read_cql_value(fixture)
        assert.same(fixture.val, decoded)
      end
    end)

    it("[set<T>]", function()
      local fixtures = helpers.cql_set_fixtures
      for i = 1, #fixtures do
        local fixture = fixtures[i]
        local buf, decoded

        buf = Buffer.new(protocol_version)
        buf:write_cql_value(fixture)
        buf:reset()
        decoded = buf:read_cql_value(fixture)
        assert.same(fixture.val, decoded)
      end
    end)

    it("[map<T, T>]", function()
      local fixtures = helpers.cql_map_fixtures
      for i = 1, #fixtures do
        local fixture = fixtures[i]
        local buf, decoded

        buf = Buffer.new(protocol_version)
        buf:write_cql_value(fixture)
        buf:reset()
        decoded = buf:read_cql_value(fixture)
        assert.same(fixture.val, decoded)
      end
    end)

    it("[tuple<T, T>]", function()
      local fixtures = helpers.cql_tuple_fixtures
      for i = 1, #fixtures do
        local fixture = fixtures[i]
        local buf, decoded

        buf = Buffer.new(protocol_version)
        buf:write_cql_value(fixture)
        buf:reset()
        decoded = buf:read_cql_value(fixture)
        assert.same(fixture.val, decoded)
      end
    end)

    it("[udt]", function()
      local fixtures = helpers.cql_udt_fixtures
      for i = 1, #fixtures do
        local fixture = fixtures[i]
        local buf, decoded

        buf = Buffer.new(protocol_version)
        buf:write_cql_value(fixture)
        buf:reset()
        decoded = buf:read_cql_value(fixture)
        assert.same(fixture.read, decoded) -- read is different from write
      end
    end)

    describe("notations", function()
      it("[string list]", function()
        local list = {"hello", "world", "goodbye"}
        local buffer = Buffer.new(protocol_version)
        buffer:write_string_list(list)
        buffer:reset()
        local decoded = buffer:read_string_list()
        assert.same(list, decoded)
      end)

      it("[string multimap]", function()
        local multimap = {
          hello = {"world", "universe"},
          goodbye = {"universe", "world"},
          foo = {"bar"}
        }
        local buffer = Buffer.new(protocol_version)
        buffer:write_string_multimap(multimap)
        buffer:reset()
        local decoded = buffer:read_string_multimap()
        assert.same(multimap, decoded)
      end)
    end)

    describe("write_cql_values()", function()
      it("writes given values and infer their types", function()
        local values = {
          true,
          42,
          "hello world",
          {"hello", "world"},
          {hello = "world"}
        }

        local buf = Buffer.new(protocol_version)
        buf:write_short(#values)
        for i = 1, #values do
          buf:write_cql_value(values[i])
        end
        buf:reset()

        assert.equal(#values, buf:read_short())
        assert.True(buf:read_cql_value             {__cql_type = cql.types.boolean})
        assert.equal(values[2], buf:read_cql_value {__cql_type = cql.types.int})
        assert.equal(values[3], buf:read_cql_value {__cql_type = cql.types.text})
        assert.same(values[4], buf:read_cql_value  {__cql_type = cql.types.set,
                                                    __cql_type_value =
                                                      {__cql_type = cql.types.text}
                                                   })
        assert.same(values[5], buf:read_cql_value  {__cql_type = cql.types.map,
                                                    __cql_type_value = {
                                                      {__cql_type = cql.types.text},
                                                      {__cql_type = cql.types.text}
                                                    }
                                                   })
      end)
    end)
  end)

  describe("CQL requests", function()
    local requests = cql.requests
    local consistencies = cql.consistencies

    it("sanity", function()
      local r = requests.query.new("SELECT * FROM peers")
      assert.equal(0, r.retries)
    end)

    describe("build_frame()", function()
      it("writes the frame's body", function()
        local r = requests.query.new("SELECT * FROM peers")
        local frame = r:build_frame(protocol_version)
        assert.is_string(frame)
      end)
      it("rebuilds the frame if called multiple times", function()
        local r = requests.query.new("SELECT * FROM local")
        local frame1 = r:build_frame(protocol_version)
        local frame2 = r:build_frame(protocol_version)
        assert.equal(frame1, frame2)
        assert.matches("SELECT * FROM local", frame1, nil, true)

        r.query = "SELECT key FROM local"

        local frame3 = r:build_frame(protocol_version)
        assert.matches("SELECT key FROM local", frame3, nil, true)
      end)
      it("sets the stream_id if provided", function()
        local r = requests.query.new("SELECT * FROM local")
        r.opts = {stream_id = 255, consistency = consistencies.one}
        local frame = r:build_frame(protocol_version)

        local header = cql.frame_reader.read_header(protocol_version, string.sub(frame, 2, -1))
        assert.equal(255, header.stream_id)
      end)
    end)

    describe("execute_prepared", function()
      it("has a 'query' attribute to allow re-preparing", function()
        local args = {123, "hello"}
        local opts = {
          page_size = 10,
          consistency = cassandra.consistencies.one
        }
        local query = "SELECT * FROM system.peers"

        local r = requests.execute_prepared.new("1234", args, opts)
        r:build_frame(protocol_version)
        assert.equal("1234", r.query_id)
        assert.same(args, r.args)
        assert.same(opts, r.opts)
        assert.is_nil(r.query)

        r = requests.execute_prepared.new("1234", args, opts, query)
        assert.equal(query, r.query)
      end)
    end)

    describe("batch", function()
      it("has a type attribute set from options", function()
        local r = requests.batch.new({}, {})
        assert.equal(1, r.type) -- unlogged

        r = requests.batch.new({}, {logged = false})
        assert.equal(1, r.type) -- unlogged

        r = requests.batch.new({}, {logged = true})
        assert.equal(0, r.type) -- logged

        r = requests.batch.new({}, {counter = true})
        assert.equal(2, r.type) -- counter
      end)
      it("has a 'queries' attribute to allow re-preparing", function()
        local opts = {
          counter = true,
          prepared = true,
          consistency = cassandra.consistencies.one
        }
        local queries = {
          {"UPDATE users SET value = value + 1 WHERE key = 'batch'", nil, "1234"},
          {"UPDATE users SET value = value + 1 WHERE key = ?", {'batch'}, "5678"},
        }

        local r = requests.batch.new(queries, opts)
        r:build_frame(protocol_version)
        assert.same(opts, r.opts)
        assert.same(queries, r.queries) -- no modifications
      end)
    end)
  end)
end
