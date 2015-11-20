local json = require "cjson"
local string_utils = require "cassandra.utils.string"
local table_concat = table.concat
local in_ngx = ngx ~= nil
local shared

-- DICT Proxy
-- https://github.com/bsm/fakengx/blob/master/fakengx.lua

local SharedDict = {}

function SharedDict:new()
  return setmetatable({data = {}}, {__index = self})
end

function SharedDict:get(key)
  return self.data[key], nil
end

function SharedDict:set(key, value)
  self.data[key] = value
  return true, nil, false
end

function SharedDict:add(key, value)
  if self.data[key] ~= nil then
    return false, "exists", false
  end

  self.data[key] = value
  return true, nil, false
end

function SharedDict:replace(key, value)
  if self.data[key] == nil then
    return false, "not found", false
  end

  self.data[key] = value
  return true, nil, false
end

function SharedDict:delete(key)
  self.data[key] = nil
end

function SharedDict:incr(key, value)
  if not self.data[key] then
    return nil, "not found"
  elseif type(self.data[key]) ~= "number" then
    return nil, "not a number"
  end

  self.data[key] = self.data[key] + value
  return self.data[key], nil
end

if in_ngx then
  shared = ngx.shared
else
  shared = {}
end

local function get_dict(shm)
  if not in_ngx then
    if shared[shm] == nil then
      shared[shm] = SharedDict:new()
    end
  end

  return shared[shm]
end

--- Hosts
-- @section hosts

local _HOSTS_KEY = "hosts"
local _SEP = ";"

local function set_hosts(shm, hosts)
  local dict = get_dict(shm)
  local ok, err = dict:set(_HOSTS_KEY, table_concat(hosts, _SEP))
  if not ok then
    err = "Cannot store hosts for cluster under shm "..shm..": "..err
  end
  return ok, err
end

local function get_hosts(shm)
  local dict = get_dict(shm)
  local value, err = dict:get(_HOSTS_KEY)
  if err then
    return nil, "Cannot retrieve hosts for cluster under shm "..shm..": "..err
  elseif value == nil then
    return nil, "Not hosts set for cluster under "..shm
  end

  return string_utils.split(value, _SEP)
end

--- Host
-- @section host

local function set_host(shm, host_addr, host)
  local dict = get_dict(shm)
  local ok, err = dict:set(host_addr, json.encode(host))
  if not ok then
    err = "Cannot store host details for cluster "..shm..": "..err
  end
  return ok, err
end

local function get_host(shm, host_addr)
  local dict = get_dict(shm)
  local value, err = dict:get(host_addr)
  if err then
    return nil, "Cannot retrieve host details for cluster under shm "..shm..": "..err
  elseif value == nil then
    return nil, "No details for host "..host_addr.." under shm "..shm
  end
  return json.decode(value)
end

--- Prepared query ids
-- @section prepared_query_ids

local function key_for_prepared_query(keyspace, query)
  return (keyspace or "").."_"..query
end

local function set_prepared_query_id(options, query, query_id)
  local shm = options.shm
  local dict = get_dict(shm)
  local prepared_key = key_for_prepared_query(options.keyspace, query)

  local ok, err = dict:set(prepared_key, query_id)
  if not ok then
    err = "Cannot store prepared query id for cluster "..shm..": "..err
  end
  return ok, err
end

local function get_prepared_query_id(options, query)
  local dict = get_dict(options.shm)
  local prepared_key = key_for_prepared_query(options.keyspace, query)

  local value, err = dict:get(prepared_key)
  if err then
    err = "Cannot retrieve prepared query id for cluster "..options.shm..": "..err
  end
  return value, err
end

return {
  get_dict = get_dict,
  get_host = get_host,
  set_host = set_host,
  set_hosts = set_hosts,
  get_hosts = get_hosts,
  set_prepared_query_id = set_prepared_query_id,
  get_prepared_query_id = get_prepared_query_id
}
