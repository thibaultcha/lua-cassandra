local say = require "say"
local types = require "cassandra.types"
local assert = require "luassert.assert"
local string_utils = require "cassandra.utils.string"

local unpack
if _VERSION == "Lua 5.3" then
  unpack = table.unpack
else
  unpack = _G.unpack
end

local _M = {}

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
  local res, err = session:execute("DROP KEYSPACE "..keyspace)
  if err then
    error(err)
  end
  return res
end

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
assert:register("assertion", "sameSet", sameSet, "assertion.sameSet.positive", "assertion.sameSet.negative")

say:set("assertion.validFixture.positive", "Fixture and decoded value do not match")
say:set("assertion.validFixture.negative", "Fixture and decoded value do not match")
assert:register("assertion", "validFixture", validFixture, "assertion.validFixture.positive", "assertion.validFixture.negative")

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

local HOSTS = os.getenv("HOSTS")
HOSTS = HOSTS and string_utils.split(HOSTS, ",") or {"127.0.0.1"}

local SMALL_LOAD = os.getenv("SMALL_LOAD") ~= nil

_M.hosts = HOSTS
_M.n_inserts = SMALL_LOAD and 1000 or 10000

return _M
