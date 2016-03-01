local utils = require "spec.spec_utils"
local cassandra = require "cassandra"

describe("Multiple nodes", function()
  local _hosts, _shm

  setup(function()
    _hosts, _shm = utils.ccm_start("multi_nodes", 3)
  end)

  describe("new()", function()
    it("stores cluster infos in shm", function()
      local _, err = cassandra.new {
        shm = _shm,
        contact_points = _hosts
      }
      assert.falsy(err)

      local cache = require "cassandra.cache"
      local hosts, err = cache.get_hosts(_shm)
      assert.falsy(err)
      assert.is_table(hosts)
      assert.equal(#_hosts, #hosts)
      for _, host_addr in ipairs(hosts) do
        local host_details, err = cache.get_host(_shm, host_addr)
        assert.falsy(err)
        assert.truthy(host_details)
      end
    end)
  end)

  describe("schema consensus", function()
    local session
    setup(function()
      local err
      session, err = cassandra.new {
        shm = _shm,
        contact_points = _hosts
      }
      assert.falsy(err)
    end)
    teardown(function()
      session:shutdown()
    end)

    it("should wait for schema consensus between multiple nodes on SCHEMA_CHANGE queries", function()
      if #_hosts < 2 then
        pending "Not testing schema consensus on single-node cluster"
      end

      local q = [[
        CREATE KEYSPACE IF NOT EXISTS resty_cassandra_spec
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': ]]..#_hosts..[[}
      ]]

      local _, err = session:execute(q)
      assert.falsy(err)

      _, err = session:execute [[
        CREATE TABLE IF NOT EXISTS resty_cassandra_spec.fixture_table(
          id uuid PRIMARY KEY,
          value varchar
        )
      ]]
      assert.falsy(err)

      -- This ought not to fail if the schema consensus was properly propagated
      -- and if we properly waited until then.
      local res, err = session:execute [[
        INSERT INTO resty_cassandra_spec.fixture_table(id, value) VALUES(uuid(), 'text')
      ]]
      assert.falsy(err)
      assert.is_table(res)
    end)
  end)
end)
