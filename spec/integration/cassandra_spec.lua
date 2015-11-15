--- Pure Lua integration tests.
-- lua-cassandra is built with support for pure Lua, outside of ngx_lua,
-- with fallback on LuaSocket when it is the case. Those integration tests must
-- mimic the ones running in ngx_lua.

local cassandra = require "cassandra"
local log = require "cassandra.log"

-- Define log level for tests
log.set_lvl("ERR")

local _shm = "cassandra"
local _contact_points = {"127.0.0.1", "127.0.0.2"}

describe("spawn cluster", function()
  it("should require a 'shm' option", function()
    assert.has_error(function()
      cassandra.spawn_cluster({
        shm = nil,
        contact_points = _contact_points
      })
    end, "shm is required for spawning a cluster/session")
  end)
  it("should spawn a cluster", function()
    local ok, err = cassandra.spawn_cluster({
      shm = _shm,
      contact_points = _contact_points
    })
    assert.falsy(err)
    assert.True(ok)
  end)
  it("should retrieve cluster infos in spawned cluster's shm", function()
    local cache = require "cassandra.cache"
    local dict = cache.get_dict(_shm)
    local hosts = cache.get_hosts(_shm)
    -- index of hosts
    assert.equal(3, #hosts)
    -- hosts details
    for _, host_addr in ipairs(hosts) do
      local host_details = cache.get_host(_shm, host_addr)
      assert.truthy(host_details)
    end
  end)
end)

describe("spawn session", function()
  it("should require a 'shm' option", function()
    assert.has_error(function()
      cassandra.spawn_session({
        shm = nil
      })
    end, "shm is required for spawning a cluster/session")
  end)
  it("should spawn a session", function()
    local session, err = cassandra.spawn_session({
      shm = _shm
    })
    assert.falsy(err)
    assert.truthy(session)
  end)
end)
