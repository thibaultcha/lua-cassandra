local host = require "cassandra.host"
local unpack = rawget(table, "unpack") or unpack

local setmetatable = setmetatable
local tonumber = tonumber
local concat = table.concat
local gsub = string.gsub
local fmt = string.format
local get_shm, new_lock, mutex, release

if ngx ~= nil then
  local shared = ngx.shared
  local resty_lock = require "resty.lock"

  get_shm = function(name)
    return shared[name]
  end
  new_lock = function(shm)
    return resty_lock:new(shm)
  end
  mutex = function(lock, key)
    local elapsed, err = lock:lock(key)
    if err then err = "could not acquire lock: "..err end
    return elapsed, err
  end
  release = function(lock)
    local ok, err = lock:unlock()
    if not ok then return nil, "could not release lock: "..err end
    return ok
  end
else
  local shared = require "cassandra.utils.shm"
  get_shm = function()return shared.new() end
  new_lock = function()end
  mutex = function()return 0 end
  release = function()end
end

local _Cluster = {}
_Cluster.__index = _Cluster

function _Cluster.new(opts)
  if not opts then opts = {} end

  local shm = get_shm(opts.shm or "cassandra")
  if not shm then return nil, "no shm named "..shm end

  local shm_prepared = get_shm(opts.shm_prepared or "cassandra_prepared")
  if not shm_prepared then return nil, "no shm named "..shm end

  new_lock(shm)

  local cluster = {
    shm = shm,
    shm_prepared = shm_prepared,

    keyspace = opts.keyspace,
    contact_points = opts.contact_points or {"127.0.0.1"},

    query_options = opts.query_options or {},
    connect_timeout = opts.connect_timeout or 1000, -- ms
    read_timeout = opts.read_timeout or 2000, -- ms
    max_schema_consensus_wait = opts.max_schema_consensus_wait or 10000, -- ms

    ssl = opts.ssl,
    ssl_verify = opts.ssl_verify,
    ssl_cert = opts.ssl_cert,
    ssl_cafile = opts.ssl_cafile,
    ssl_key = opts.ssl_key,
    auth = opts.auth,

    load_balancing_policy = nil,
    reconnection_policy = nil,
    retry_policy = nil
  }

  return setmetatable(cluster, _Cluster)
end

--- shm cluster infos

local function split(str, separator)
  local sep, fields = separator or ":", {}
  local pattern = fmt("([^%s]+)", sep)
  gsub(str, pattern, function(c) fields[#fields+1] = c end)
  return fields
end

local _peers_key = "peers"
local _sep = ";"

function _Cluster:set_peers(peers)
  local ok, err = self.shm:safe_set(_peers_key, concat(peers, _sep))
  if not ok then return nil, "cannot set peers: "..err end
  return ok
end

function _Cluster:peers()
  local peers_addresses, err = self.shm:get(_peers_key)
  if err then
    return nil, "cannot get peers: "..err
  elseif not peers_addresses then
    return nil, "no peers in shm, must refresh"
  else
    return split(peers_addresses, _sep)
  end
end

function _Cluster:set_peer(address, infos)
  local ok, err = self.shm:safe_set(address,
                    infos.unhealthy_at.._sep..infos.reconnection_delay)
  if not ok then return nil, "cannot set peer infos: "..err end
  return ok
end

function _Cluster:get_peer(address)
  local v, err = self.shm:get(address)
  if err then
    return nil, "cannot get peer infos: "..err
  elseif not v then
    return nil, "no infos in shm for this peer, must refresh"
  else
    local infos = split(v, _sep)
    return {
      unhealthy_at = tonumber(infos[1]),
      reconnection_delay = tonumber(infos[2])
    }
  end
end

--- Coordinator stuff

local function no_host_available_error(errors)
  local buf = {}
  for address, err in pairs(errors) do
    buf[#buf + 1] = address..": "..err
  end
  return "all hosts tried for query failed. "..concat(buf, "")
end

local function serialize_contact_points(contact_points)
  local buf = {}
  for _, contact_point in ipairs(contact_points) do
    local address, port = unpack(split(contact_point, ":"))
    buf[#buf + 1] = {address = address, port = port}
  end
  return buf
end

local function get_first_coordinator(self)
  local errors = {}
  local contact_points = serialize_contact_points(self.contact_points)
  for _, cp in ipairs(contact_points) do
    local peer, err = host.new {
      host = cp.address,
      port = cp.port,
      ssl = self.ssl,
      verify = self.ssl_verify,
      cert = self.ssl_cert,
      cafile = self.ssl_cafile,
      key = self.ssl_key,
      auth = self.auth,
    }
    if not peer then return nil, err end
    local ok, err, maybe_down = peer:connect()
    if not ok then
      if maybe_down then errors[cp.address] = err
      else return nil, err end
    else
      -- our coordinator
      return peer
    end
  end

  return nil, no_host_available_error(errors)
end

local SELECT_PEERS = [[
SELECT peer,data_center,rack,rpc_address FROM system.peers
]]

function _Cluster:refresh()
  local elapsed, err = mutex(self.shm)
  if not elapsed then return nil, err
  elseif elapsed == 0 then
    local coordinator, err = get_first_coordinator(self)
    if not coordinator then return nil, err end

    local rows, err = coordinator:execute(SELECT_PEERS)
    if not rows then return nil, err end

    coordinator:setkeepalive()

    rows[#rows + 1] = {rpc_address = coordinator.host}
    local hosts = {}
    for _, row in ipairs(rows) do
      hosts[#hosts + 1] = row.rpc_address
      local ok, err = self:set_peer(row.rpc_address, {
        unhealthy_at = 0,
        reconnection_delay = 0
      })
      if not ok then return nil, err end
    end

    local ok, err = self:set_peers(hosts)
    if not ok then return nil, err end

    release(self.shm)
  end

  return true
end

return _Cluster
