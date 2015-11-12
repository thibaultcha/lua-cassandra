local json = require "cjson"
local log = require "cassandra.log"
local string_utils = require "cassandra.utils.string"
local time_utils = require "cassandra.utils.time"
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
  return self.data[key], 0
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
    log.err("Cannot store hosts: "..err)
  end
end

local function get_hosts(shm)
  local dict = get_dict(shm)
  local value, err = dict:get(_HOSTS_KEY)
  if err then
    log.err("Cannot retrieve hosts: "..err)
  end
  return string_utils.split(value, _SEP)
end

--- Host
-- @section host

local function set_host(shm, host_addr, host)
  local dict = get_dict(shm)
  local ok, err = dict:set(host_addr, json.encode(host))
  if not ok then
    log.err("Cannot store hosts: "..err)
  end
end

local function get_host(shm, host_addr)
  local dict = get_dict(shm)
  local value, err = dict:get(host_addr)
  if err then
    log.err("Cannot retrieve host: "..err)
  elseif value then
    return json.decode(value)
  end
end

local function set_host_down(shm, host_addr)
  log.warn("Setting host "..host_addr.." as DOWN")
  local host = get_host(shm, host_addr)
  host.unhealthy_at = time_utils.get_time()
  set_host(shm, host_addr, host)
end

local function set_host_up(shm, host_addr)
  log.info("Setting host "..host_addr.." as UP")
  local host = get_host(shm, host_addr)
  host.unhealthy_at = 0
  set_host(shm, host_addr, host)
end

local function is_host_up(shm, host_addr)
  local host = get_host(shm, host_addr)
  return host.unhealthy_at == 0
end

local function can_host_be_considered_up(shm, host_addr)
  local host = get_host(shm, host_addr)
  return is_host_up(shm, host_addr) or (time_utils.get_time() - host.unhealthy_at > host.reconnection_delay)
end

return {
  get_dict = get_dict,
  get_host = get_host,
  set_host = set_host,
  set_hosts = set_hosts,
  get_hosts = get_hosts,
  set_host_up = set_host_up,
  set_host_down = set_host_down,
  is_host_up = is_host_up,
  can_host_be_considered_up = can_host_be_considered_up
}
