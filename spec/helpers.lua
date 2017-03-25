local unpack = rawget(table, "unpack") or unpack

local function exec(cmd, ignore)
  local res1 = os.execute(cmd.." >/dev/null")
  local ok
  if _VERSION == "Lua 5.1" then
    ok = res1 == 0
  else
    ok = not not res1
  end
  if not ok and not ignore then
    error("non-0 exit code: "..cmd, 2)
    os.exit(1)
  end
  return ok
end

local _M = {
  cassandra_version = os.getenv("CASSANDRA") or "3.10",
  ssl_path = os.getenv("SSL_PATH") or "spec/fixtures/ssl"
}

local function num(v)
  local maj, min, patch = string.match(v, "^(%d*)%.(%d*)%.?(%d*)$")
  local str = string.format("%02d%02d%02d",
                            tonumber(maj),
                            tonumber(min),
                            patch and tonumber(patch) or 0)
  return tonumber(str)
end

_M.cassandra_version_num = num(_M.cassandra_version)

--- CCM

local function ccm_exists(c_name)
  return exec("ccm list | grep "..c_name, true)
end

local function ccm_is_current(c_name)
  return exec("ccm list | grep '*"..c_name.."'", true)
end

function _M.ccm_start(opts)
  opts = opts or {}
  opts.name = opts.name or "default"
  opts.version = opts.version or _M.cassandra_version
  opts.nodes = opts.nodes or 1

  local cluster_name = "lua_cassandra_"..opts.name.."_specs"

  if not ccm_is_current(cluster_name) then
    exec("ccm stop", true)
  end

  -- create cluster if not exists
  if not ccm_exists(cluster_name) then
    local cmd = string.format("ccm create %s -v binary:%s -n %s",
                              cluster_name, opts.version, opts.nodes)
    if opts.ssl then
      cmd = cmd.." --ssl='".._M.ssl_path.."'"
    end
    if opts.require_client_auth then
      cmd = cmd.." --require_client_auth"
    end
    if opts.pwd_auth then
      cmd = cmd.." --pwd-auth"
    end

    exec(cmd)
  end

  exec("ccm switch "..cluster_name)
  if _M.cassandra_version_num >= 30000 then
    exec("ccm updateconf 'enable_user_defined_functions: true'")
  end
  exec("ccm start --wait-for-binary-proto")

  local hosts = {}
  for i = 1, opts.nodes do
    hosts[#hosts+1] = "127.0.0."..i
  end

  if opts.pwd_auth then
    -- the cassandra superuser takes some time to be created
    exec("sleep 10")
  end

  return hosts
end

--- CQL

function _M.create_keyspace(host, keyspace)
  return host:execute([[
    CREATE KEYSPACE IF NOT EXISTS ]]..keyspace..[[
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
  ]])
end

function _M.drop_keyspace(host, keyspace)
  return host:execute("DROP KEYSPACE "..keyspace)
end

--- Assertions

local say = require "say"
local luassert = require "luassert.assert"

local delta = 0.0000001
local function fixture(state, arguments)
  local ok
  local fixture_type, fixture, decoded = unpack(arguments)
  if fixture_type == "float" then
    ok = math.abs(decoded - fixture) < delta
  elseif type(fixture) == "table" then
    ok = pcall(luassert.same, fixture, decoded)
  else
    ok = pcall(luassert.equal, fixture, decoded)
  end
  -- pop first argument, for proper output message (like luassert.same)
  table.remove(arguments, 1)
  table.insert(arguments, 1, table.remove(arguments, 2))
  return ok
end

local function same_set(state, arguments)
  local fixture, decoded = unpack(arguments)

  for _, x in ipairs(fixture) do
    local has = false
    for _, y in ipairs(decoded) do
      if y == x then
        has = true
        break
      end
    end
    if not has then
      return false
    end
  end

  return true
end

say:set("assertion.same_set.positive", "Fixture and decoded value do not match")
say:set("assertion.same_set.negative", "Fixture and decoded value do not match")
luassert:register("assertion", "same_set", same_set,
                  "assertion.same_set.positive",
                  "assertion.same_set.negative")

say:set("assertion.fixture.positive",
        "Expected fixture and decoded value to match.\nPassed in:\n%s\nExpected:\n%s")
say:set("assertion.fixture.negative",
        "Expected fixture and decoded value to not match.\nPassed in:\n%s\nExpected:\n%s")
luassert:register("assertion", "fixture", fixture,
                  "assertion.fixture.positive",
                  "assertion.fixture.negative")

--- Fixtures

local cql = require "cassandra.cql"

_M.cql_fixtures = {
  -- custom
  ascii = {"Hello world", ""},
  bigint = {0, 42, -42, 42000000000},
  boolean = {true, false},
  -- counter
  double = {0, 1.0000000000000004, -1.0000000000000004},
  float = {0, 3.14151, -3.14151},
  inet = {
    "127.0.0.1", "0.0.0.1", "8.8.8.8",
    "2001:0db8:85a3:0042:1000:8a2e:0370:7334",
    "2001:0db8:0000:0000:0000:0000:0000:0001"
  },
  int = {0, 4200, -42},
  text = {"Hello world", ""},
  -- list
  -- map
  -- set
  uuid = {"1144bada-852c-11e3-89fb-e0b9a54a6d11"},
  timestamp = {1405356926},
  varchar = {"Hello world", ""},
  varint = {0, 4200, -42},
  timeuuid = {"1144bada-852c-11e3-89fb-e0b9a54a6d11"}
  -- udt
  -- tuple
}

_M.cql_list_fixtures = {
  {
    val = {"abc", "def"},
    name = "list_sample_text",
    __cql_type = cql.types.list,
    __cql_type_value = {__cql_type = cql.types.text},
  },
  {
    val = {1, 2 , 0, -42, 42},
    name = "list_sample_int",
    __cql_type = cql.types.list,
    __cql_type_value = {__cql_type = cql.types.int},
  }
}

_M.cql_set_fixtures = {
  {
    val = {"xyz", "abc"},
    name = "set_sample_text",
    __cql_type = cql.types.list,
    __cql_type_value = {__cql_type = cql.types.text},
  },
  {
    val = {0, 1, 2, 3, 4},
    name = "set_sample_int",
    __cql_type = cql.types.list,
    __cql_type_value = {__cql_type = cql.types.int},
  }
}

_M.cql_map_fixtures = {
  {
    val = {k1 = "v1", k2 = "v2"},
    name = "map_sample_text_text",
    __cql_type = cql.types.map,
    __cql_type_value = {{__cql_type = cql.types.text}, {__cql_type = cql.types.text}}
  },
  {
    val = {k1 = 1, k2 = 2},
    name = "map_sample_text_int",
    __cql_type = cql.types.map,
    __cql_type_value = {{__cql_type = cql.types.text}, {__cql_type = cql.types.int}}
  }
}

_M.cql_tuple_fixtures = {
  {
    val = {"world", "hello"},
    __cql_type = cql.types.tuple,
    __cql_type_value = {
      fields = {
        {__cql_type = cql.types.text}, {__cql_type = cql.types.text},
      }
    }
  },
  {
    val = {"hello", "world"},
    __cql_type = cql.types.tuple,
    __cql_type_value = {
      fields = {
        {__cql_type = cql.types.text}, {__cql_type = cql.types.text},
      }
    }
  },
  --[[
  {
    val = {"hello", 1},
    __cql_type = cql.types.tuple,
    __cql_type_value = {
      fields = {
        {__cql_type = cql.types.text}, {__cql_type = cql.types.int},
      }
    }
  }
--]]
}

_M.cql_udt_fixtures = {
  {
    val = {"v1", "v2"},
    read = {field1 = "v1", field2 = "v2"},
    __cql_type = cql.types.udt,
    __cql_type_value = {
      udt_keyspace = "",
      udt_name = "",
      fields = {
        {name = "field1", type = {__cql_type = cql.types.text}},
        {name = "field2", type = {__cql_type = cql.types.text}},
      }
    }
  }
}

_M.keyspace = "lua_resty_specs"

return _M
