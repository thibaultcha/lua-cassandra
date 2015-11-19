local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

local _shm = "cql_types"
local _KEYSPACE = "resty_cassandra_cql_types_specs"

-- Define log level for tests
utils.set_log_lvl("ERR")

describe("CQL types integration", function()
  local session

  setup(function()
    local _, err = cassandra.spawn_cluster({
      shm = _shm,
      contact_points = utils.contact_points
    })
    assert.falsy(err)

    session, err = cassandra.spawn_session({shm = _shm})
    assert.falsy(err)

    utils.create_keyspace(session, _KEYSPACE)

    _, err = session:set_keyspace(_KEYSPACE)
    assert.falsy(err)

    _, err = session:execute [[
      CREATE TYPE address(
        street text,
        city text,
        zip int,
        country text
      )
    ]]
    assert.falsy(err)

    _, err = session:execute [[
      CREATE TABLE IF NOT EXISTS all_types(
        id uuid PRIMARY KEY,
        ascii_sample ascii,
        bigint_sample bigint,
        blob_sample blob,
        boolean_sample boolean,
        decimal_sample decimal,
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
    ]]
    assert.falsy(err)

    utils.wait()
  end)

  teardown(function()
    utils.drop_keyspace(session, _KEYSPACE)
    session:shutdown()
  end)

  local _UUID = "1144bada-852c-11e3-89fb-e0b9a54a6d11"

  for fixture_type, fixture_values in pairs(utils.cql_fixtures) do
    it("["..fixture_type.."] should be inserted and retrieved", function()
      local insert_query = string.format("INSERT INTO all_types(id, %s_sample) VALUES(?, ?)", fixture_type)
      local select_query = string.format("SELECT %s_sample FROM all_types WHERE id = ?", fixture_type)

      for _, fixture in ipairs(fixture_values) do
        local res, err = session:execute(insert_query, {cassandra.uuid(_UUID), cassandra[fixture_type](fixture)})
        assert.falsy(err)
        assert.truthy(res)

        local rows, err = session:execute(select_query, {cassandra.uuid(_UUID)})
        assert.falsy(err)
        assert.truthy(rows)

        local decoded = rows[1][fixture_type.."_sample"]
        assert.validFixture(fixture_type, fixture, decoded)
      end
    end)
  end

  it("[list<type>] should be inserted and retrieved", function()
    for _, fixture in ipairs(utils.cql_map_fixtures) do
      local insert_query = string.format("INSERT INTO all_types(id, map_sample_%s_%s) VALUES(?, ?)", fixture.key_type_name, fixture.value_type_name)
      local select_query = string.format("SELECT map_sample_%s_%s FROM all_types WHERE id = ?", fixture.key_type_name, fixture.value_type_name)

      local res, err = session:execute(insert_query, {cassandra.uuid(_UUID), cassandra.map(fixture.value)})
      assert.falsy(err)
      assert.truthy(res)

      local rows, err = session:execute(select_query, {cassandra.uuid(_UUID)})
      assert.falsy(err)
      assert.truthy(rows)

      local decoded = rows[1]["map_sample_"..fixture.key_type_name.."_"..fixture.value_type_name]
      assert.validFixture("list", fixture.value, decoded)
    end
  end)

  it("[map<type, type>] should be inserted and retrieved", function()
    for _, fixture in ipairs(utils.cql_list_fixtures) do
      local insert_query = string.format("INSERT INTO all_types(id, list_sample_%s) VALUES(?, ?)", fixture.type_name)
      local select_query = string.format("SELECT list_sample_%s FROM all_types WHERE id = ?", fixture.type_name)

      local res, err = session:execute(insert_query, {cassandra.uuid(_UUID), cassandra.list(fixture.value)})
      assert.falsy(err)
      assert.truthy(res)

      local rows, err = session:execute(select_query, {cassandra.uuid(_UUID)})
      assert.falsy(err)
      assert.truthy(rows)

      local decoded = rows[1]["list_sample_"..fixture.type_name]
      assert.validFixture("list", fixture.value, decoded)
    end
  end)

  it("[set<type>] should be inserted and retrieved", function()
    for _, fixture in ipairs(utils.cql_list_fixtures) do
      local insert_query = string.format("INSERT INTO all_types(id, set_sample_%s) VALUES(?, ?)", fixture.type_name)
      local select_query = string.format("SELECT set_sample_%s FROM all_types WHERE id = ?", fixture.type_name)

      local res, err = session:execute(insert_query, {cassandra.uuid(_UUID), cassandra.set(fixture.value)})
      assert.falsy(err)
      assert.truthy(res)

      local rows, err = session:execute(select_query, {cassandra.uuid(_UUID)})
      assert.falsy(err)
      assert.truthy(rows)

      local decoded = rows[1]["set_sample_"..fixture.type_name]
      assert.sameSet(fixture.value, decoded)
    end
  end)

  it("[udt] should be inserted and retrieved", function()
    local res, err = session:execute("INSERT INTO all_types(id, udt_sample) VALUES(?, ?)",
      {cassandra.uuid(_UUID), cassandra.udt({"montgomery st", "san francisco", 94111, nil})})
    assert.falsy(err)
    assert.truthy(res)

    local rows, err = session:execute("SELECT udt_sample FROM all_types WHERE id = ?", {cassandra.uuid(_UUID)})
    assert.falsy(err)
    assert.truthy(rows)
    local address = rows[1].udt_sample
    assert.equal("montgomery st", address.street)
    assert.equal("san francisco", address.city)
    assert.equal(94111, address.zip)
  end)

  it("[tuple] should be inserted and retrieved", function()
    for _, fixture in ipairs(utils.cql_tuple_fixtures) do
      local res, err = session:execute("INSERT INTO all_types(id, tuple_sample) VALUES(?, ?)", {cassandra.uuid(_UUID), cassandra.tuple(fixture.value)})
      assert.falsy(err)
      assert.truthy(res)

      local rows, err = session:execute("SELECT tuple_sample FROM all_types WHERE id = ?", {cassandra.uuid(_UUID)})
      assert.falsy(err)
      assert.truthy(rows)
      local tuple = rows[1].tuple_sample
      assert.equal(fixture.value[1], tuple[1])
      assert.equal(fixture.value[2], tuple[2])
    end
  end)
end)
