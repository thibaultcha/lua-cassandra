local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

describe("batch()", function()
  local session

  setup(function()
    local _, err
    local hosts, shm = utils.ccm_start()

    session, err = cassandra.spawn_session {
      shm = shm,
      contact_points = hosts
    }
    assert.falsy(err)

    utils.create_keyspace(session, shm)
    _, err = session:set_keyspace(shm)
    assert.falsy(err)

    _, err = session:execute [[
      CREATE TABLE IF NOT EXISTS users(
        id uuid,
        name varchar,
        n int,
        PRIMARY KEY(id, n)
      )
    ]]
    assert.falsy(err)

    _, err = session:execute [[
      CREATE TABLE IF NOT EXISTS counter_test_table(
        key text PRIMARY KEY,
        value counter
      )
    ]]
    assert.falsy(err)
  end)

  after_each(function()
    session:execute("TRUNCATE counter_test_table")
  end)

  local _UUID = "ca002f0a-8fe4-11e5-9663-43d80ec97d3e"

  it("should execute logged batched queries with no params", function()
    local res, err = session:batch({
      {"INSERT INTO users(id, name, n) VALUES(".._UUID..", 'Alice', 1)"},
      {"UPDATE users SET name = 'Alice' WHERE id = ".._UUID.." AND n = 1"},
      {"UPDATE users SET name = 'Alicia' WHERE id = ".._UUID.." AND n = 1"}
    })
    assert.falsy(err)
    assert.is_table(res)
    assert.equal("VOID", res.type)

    local rows, err = session:execute("SELECT * FROM users WHERE id = ? AND n = 1", {cassandra.uuid(_UUID)})
    assert.falsy(err)
    assert.is_table(rows)
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
    assert.is_table(res)
    assert.equal("VOID", res.type)

    local rows, err = session:execute("SELECT * FROM users WHERE id = ? AND n = 2", {cassandra.uuid(_UUID)})
    assert.falsy(err)
    assert.is_table(rows)
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
    assert.is_table(res)
    assert.equal("VOID", res.type)

    local rows, err = session:execute("SELECT * FROM users WHERE id = ? AND n = 3", {cassandra.uuid(_UUID)})
    assert.falsy(err)
    assert.is_table(rows)
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
    assert.is_table(res)
    assert.equal("VOID", res.type)

    local rows, err = session:execute("SELECT value FROM counter_test_table WHERE key = 'counter'")
    assert.falsy(err)
    assert.is_table(rows)
    local row = rows[1]
    assert.equal(3, row.value)
  end)
  it("should return any error", function()
    local _, err = session:batch({
      {"INSERT WHATEVER"},
      {"INSERT THING"}
    })
    assert.is_table(err)
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
    assert.is_table(rows)
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
    assert.is_table(rows)
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
    assert.is_table(rows)
    local row = rows[1]
    assert.equal("Alicia6", row.name)
  end)
end)
