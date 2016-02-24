local log = require "cassandra.log"
local Errors = require "cassandra.errors"
local string_utils = require "cassandra.utils.string"

local table_concat = table.concat
local tonumber = tonumber

local in_ngx = ngx ~= nil
local dicts = {}

-- ngx.shared.DICT proxy
-- https://github.com/bsm/fakengx/blob/master/fakengx.lua
-- Used when the driver is required outside of ngx_lua.
-- Eventually, attaching the cluster infos to the session
-- should be done standardly, but this is a faster alternative
-- for now.

local SharedDict = {}

local function set(data, key, value)
  data[key] = {
    value = value,
    info = {expired = false}
  }
end

function SharedDict:new()
  return setmetatable({data = {}}, {__index = self})
end

function SharedDict:get(key)
  return self.data[key] and self.data[key].value, nil
end

function SharedDict:set(key, value)
  set(self.data, key, value)
  return true, nil, false
end

function SharedDict:get_keys()
  local keys = {}
  for k in pairs(self.data) do
    keys[#keys + 1] = k
  end
  return keys
end

SharedDict.safe_set = SharedDict.set

function SharedDict:add(key, value)
  if self.data[key] ~= nil then
    return false, "exists", false
  end

  set(self.data, key, value)
  return true, nil, false
end

function SharedDict:replace(key, value)
  if self.data[key] == nil then
    return false, "not found", false
  end

  set(self.data, key, value)
  return true, nil, false
end

function SharedDict:delete(key)
  self.data[key] = nil
end

function SharedDict:incr(key, value)
  if not self.data[key] then
    return nil, "not found"
  elseif type(self.data[key].value) ~= "number" then
    return nil, "not a number"
  end

  self.data[key].value = self.data[key].value + value
  return self.data[key].value, nil
end

function SharedDict:flush_all()
  for _, item in pairs(self.data) do
    item.info.expired = true
  end
end

function SharedDict:flush_expired(n)
  local data = self.data
  local flushed = 0

  for key, item in pairs(self.data) do
    if item.info.expired then
      data[key] = nil
      flushed = flushed + 1
      if n and flushed == n then
        break
      end
    end
  end

  self.data = data

  return flushed
end

local function get_dict(shm)
  local dict = dicts[shm]

  if dict == nil then
    if in_ngx then
      dict = ngx.shared[shm]
      if dict == nil then
        error("No shared dict named "..shm)
      end
    else
      dict = SharedDict:new()
    end
    dicts[shm] = dict
  end

  return dict
end

--- Hosts
-- @section hosts

local _HOSTS_KEY = "hosts"
local _SEP = ";"

local function set_hosts(shm, hosts)
  local dict = get_dict(shm)
  local ok, err = dict:safe_set(_HOSTS_KEY, table_concat(hosts, _SEP))
  if not ok then
    return false, Errors.shm(shm, "cannot store hosts ("..err..")")
  end
  return true
end

local function get_hosts(shm)
  local dict = get_dict(shm)
  local host_addresses, err = dict:get(_HOSTS_KEY)
  if err then
    return nil, Errors.shm(shm, "cannot retrieve hosts ("..err..")")
  elseif host_addresses ~= nil then
    return string_utils.split(host_addresses, _SEP)
  end
end

--- Host
-- @section host

local function set_host(shm, host_addr, host)
  local dict = get_dict(shm)
  local ok, err = dict:safe_set(host_addr, host.unhealthy_at.._SEP..host.reconnection_delay)
  if not ok then
    return false, Errors.shm(shm, "cannot store host details ("..err..")")
  end
  return true
end

local function get_host(shm, host_addr)
  local dict = get_dict(shm)
  local value, err = dict:get(host_addr)
  if err then
    return nil, Errors.shm(shm, "cannot retrieve host details ("..err..")")
  elseif value == nil then
    return nil, Errors.internal_driver("no details for host "..host_addr.." under shm "..shm)
  end

  local h = string_utils.split(value, _SEP)
  return {
    unhealthy_at = tonumber(h[1]),
    reconnection_delay = tonumber(h[2])
  }
end

--- Prepared query ids
-- @section prepared_query_ids

local function key_for_prepared_query(keyspace, query)
  return (keyspace or "").."_"..query
end

local function set_prepared_query_id(options, query, query_id)
  if options.prepared_shm == options.shm then
    log.warn "same shm used for cluster infos and prepared statements, consider using different ones"
  end

  local shm = options.prepared_shm
  local dict = get_dict(shm)
  local prepared_key = key_for_prepared_query(options.keyspace, query)

  local ok, err, forcible = dict:set(prepared_key, query_id)
  if not ok then
    return false, Errors.shm(shm, "cannot store prepared query id ("..err..")")
  elseif forcible then
    log.warn("shm for prepared queries '"..shm.."' is running out of memory, consider increasing its size")
    dict:flush_expired(1) -- flush oldest query
  end
  return true
end

local function get_prepared_query_id(options, query)
  local shm = options.prepared_shm
  local dict = get_dict(shm)
  local prepared_key = key_for_prepared_query(options.keyspace, query)

  local value, err = dict:get(prepared_key)
  if err then
    return nil, Errors.shm(shm, "cannot retrieve prepared query id ("..err..")")
  end
  return value, nil, prepared_key
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
