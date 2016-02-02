local unpack
if _VERSION == "Lua 5.3" then
  unpack = table.unpack
else
  unpack = _G.unpack
end

local function exec(cmd, ignore)
  cmd = cmd.." >/dev/null"
  local ok
  if _VERSION == "Lua 5.1" then
    ok = select(1, os.execute(cmd)) == 0
  else
    ok = select(3, os.execute(cmd)) == 0
  end

  if not ok and not ignore then
    os.exit(1)
  end

  return ok
end

local _M = {}

local LOAD = os.getenv("CASSANDRA_LOAD")
local SSL_PATH = os.getenv("SSL_PATH") or "spec/fixtures/ssl"

_M.n_inserts = LOAD and tonumber(LOAD) or 1000
_M.CASSANDRA_VERSION = os.getenv("CASSANDRA") or "2.1.12"

--- CCM

function _M.ssl_path()
  return SSL_PATH
end

function _M.ccm_exists(c_name)
  return exec("ccm list | grep "..c_name, true)
end

function _M.is_current(c_name)
  return exec("ccm list | grep '*"..c_name.."'", true)
end

function _M.ccm_start(c_name, n_nodes, c_ver, opts)
  if not c_name then c_name = "default" end
  if not n_nodes then n_nodes = 1 end
  if not c_ver then c_ver = _M.CASSANDRA_VERSION end

  c_name = "lua_cassandra_"..c_name.."_specs"

  if not _M.is_current(c_name) then
    exec("ccm stop", true)
  end

  -- create cluster if not exists
  if not _M.ccm_exists(c_name) then
    local cmd = string.format("ccm create %s -v binary:%s -n %s", c_name, c_ver, n_nodes)

    if opts and opts.ssl then
      cmd = cmd.." --ssl='".._M.ssl_path().."'"
    end

    if opts and opts.require_client_auth then
      cmd = cmd.." --require_client_auth"
    end

    if opts and opts.pwd_auth then
      cmd = cmd.." --pwd-auth"
    end

    exec(cmd)
  end

  exec("ccm switch "..c_name)
  exec("ccm start --wait-for-binary-proto")

  local hosts = {}
  for i = 1, n_nodes do
    hosts[#hosts + 1] = "127.0.0."..i
  end

  if opts and opts.pwd_auth then
    -- the cassandra superuser takes some time to be created
    os.execute("sleep 10")
  end

  return hosts, c_name
end

--- CQL

function _M.create_keyspace(session, keyspace)
  local res, err = session:execute([[
    CREATE KEYSPACE IF NOT EXISTS ]]..keyspace..[[
    WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
  ]])
  if err then
    error(err)
  end

  return res
end

function _M.drop_keyspace(session, keyspace)
  session:execute("DROP KEYSPACE "..keyspace)
end

--- Assertions

local say = require "say"
local assert = require "luassert.assert"

local delta = 0.0000001
local function validFixture(state, arguments)
  local fixture_type, fixture, decoded = unpack(arguments)

  local result
  if fixture_type == "float" then
    result = math.abs(decoded - fixture) < delta
  elseif type(fixture) == "table" then
    result = pcall(assert.same, fixture, decoded)
  else
    result = pcall(assert.equal, fixture, decoded)
  end

  -- pop first argument, for proper output message (like assert.same)
  table.remove(arguments, 1)
  table.insert(arguments, 1, table.remove(arguments, 2))

  return result
end

local function sameSet(state, arguments)
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

say:set("assertion.sameSet.positive", "Fixture and decoded value do not match")
say:set("assertion.sameSet.negative", "Fixture and decoded value do not match")
assert:register("assertion",
                "sameSet",
                sameSet,
                "assertion.sameSet.positive",
                "assertion.sameSet.negative")

say:set("assertion.validFixture.positive",
        "Expected fixture and decoded value to match.\nPassed in:\n%s\nExpected:\n%s")
say:set("assertion.validFixture.negative",
        "Expected fixture and decoded value to not match.\nPassed in:\n%s\nExpected:\n%s")
assert:register("assertion",
                "validFixture",
                validFixture,
                "assertion.validFixture.positive",
                "assertion.validFixture.negative")

--- Fixtures

local types = require "cassandra.types"

_M.cql_fixtures = {
  -- custom
  ascii = {"Hello world", ""},
  bigint = {0, 42, -42, 42000000000, -42000000000},
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
  -- uuid
  timestamp = {1405356926},
  varchar = {"Hello world", ""},
  varint = {0, 4200, -42},
  timeuuid = {"1144bada-852c-11e3-89fb-e0b9a54a6d11"}
  -- udt
  -- tuple
}

_M.cql_list_fixtures = {
  {value_type = types.cql_types.text, type_name = "text", value = {"abc", "def"}},
  {value_type = types.cql_types.int, type_name = "int", value = {1, 2 , 0, -42, 42}}
}

_M.cql_map_fixtures = {
  {
   key_type = types.cql_types.text,
   key_type_name = "text",
   value_type = types.cql_types.text,
   value_type_name = "text",
   value = {k1 = "v1", k2 = "v2"}
  },
  {
   key_type = types.cql_types.text,
   key_type_name = "text",
   value_type = types.cql_types.int,
   value_type_name = "int",
   value = {k1 = 1, k2 = 2}
  }
}

_M.cql_set_fixtures = _M.cql_list_fixtures

_M.cql_tuple_fixtures = {
  {type = {"text", "text"}, value = {"hello", "world"}},
  {type = {"text", "text"}, value = {"world", "hello"}}
}

return _M
