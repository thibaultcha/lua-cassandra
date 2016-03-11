local host = require "cassandra.host"
local time_utils = require "cassandra.utils.time"

local unpack = rawget(table, "unpack") or unpack
local setmetatable = setmetatable
local tonumber = tonumber
local concat = table.concat
local ipairs = ipairs
local pairs = pairs
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
  opts = opts or {}
  local shm = get_shm(opts.shm or "cassandra")
  if not shm then return nil, "no shm named "..shm end

  local shm_prepared = get_shm(opts.shm_prepared or "cassandra_prepared")
  if not shm_prepared then return nil, "no shm named "..shm end

  local cluster = {
    shm = shm,
    shm_prepared = shm_prepared,
    lock = new_lock(shm),

    keyspace = opts.keyspace,
    contact_points = opts.contact_points or {"127.0.0.1"},

    query_options = opts.query_options or {},
    read_timeout = opts.read_timeout or 2000, -- ms
    connect_timeout = opts.connect_timeout or 1000, -- ms
    max_schema_consensus_wait = opts.max_schema_consensus_wait or 10000, -- ms

    ssl = opts.ssl,
    ssl_verify = opts.ssl_verify,
    ssl_cafile = opts.ssl_cafile,
    ssl_cert = opts.ssl_cert,
    ssl_key = opts.ssl_key,
    auth = opts.auth,

    load_balancing_policy = opts.load_balancing_policy
      or require("cassandra.policies.load_balancing").shm_round_robin,
    reconnection_policy = nil,
    retry_policy = nil
  }

  return setmetatable(cluster, _Cluster)
end

function _Cluster:spawn_host(address, port)
  return host.new {
    host = address,
    port = port,
    keyspace = self.keyspace,
    ssl = self.ssl,
    verify = self.ssl_verify,
    cert = self.ssl_cert,
    cafile = self.ssl_cafile,
    key = self.ssl_key,
    auth = self.auth,
  }
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

function _Cluster:set_prepared(query, query_id)
  local key = (self.keyspace or "").."_"..query
  local ok, err, forcible = self.shm_prepared:set(key, query_id)
  if not ok then
    return nil, "cannot set prepared query id: "..err
  elseif forcible then
    -- TODO: warn
  end
  return ok
end

function _Cluster:get_prepared(query)
  local key = (self.keyspace or "").."_"..query
  local query_id, err = self.shm_prepared:get(key)
  if err then
    return nil, "cannot get prepared query id: "..err
  end
  return query_id
end

--- Peer health stuff

local function set_peer_down(self, address, peer_infos)
  local elapsed, err = mutex(self.lock)
  if not elapsed then return nil, err
  elseif elapsed == 0 then
    peer_infos.unhealthy_at = time_utils.now()
    -- TODO: reconnection delay
    local ok, err = self:set_peer(address, peer_infos)
    if not ok then return nil, err end
    release(self.lock)
  end

  return true
end

local function is_peer_healthy(self, address, port, check_shm)
  local peer_infos
  if check_shm then
    -- must get most up-to-date infos about that peer,
    -- from the shm zone. Maybe another worker reported
    -- it unhealthy
    local err
    peer_infos, err = self:get_peer(address)
    if not peer_infos then return nil, err end

    if peer_infos.unhealthy_at > 0 or
      time_utils.now() - peer_infos.unhealthy_at < peer_infos.reconnection_delay then
      -- this host is already reported as down, still waiting for retry
      return nil, "host considered DOWN"
    end
  end

  -- host seems healthy, let's try it
  local peer, err = self:spawn_host(address, port)
  if not peer then return nil, err end

  peer:settimeout(self.connect_timeout)

  local ok, err, maybe_down = peer:connect()
  if ok then
    -- our coordinator
    return peer
  else
    peer:close()
    if maybe_down then
      -- host seems unhealthy
      if check_shm then
        local ok, err = set_peer_down(self, address, peer_infos)
        if not ok then return nil, err end
      end
      return nil, "host seems unhealthy: "..err -- err from :connect()
    end
    return nil, err -- err from :connect()
  end
end

--- Coordinator stuff

local function no_host_available_error(errors)
  local buf = {"all hosts tried for query failed."}
  for address, err in pairs(errors) do
    buf[#buf + 1] = address..": "..err
  end
  return concat(buf, " ")
end

local function serialize_contact_points(contact_points)
  local buf = {}
  for _, contact_point in ipairs(contact_points) do
    local address, port = unpack(split(contact_point, ":"))
    buf[#buf + 1] = {address = address, port = port}
  end
  return buf
end

function _Cluster:get_first_coordinator(contact_points)
  local errors = {}
  contact_points = serialize_contact_points(contact_points)

  for _, cp in ipairs(contact_points) do
    local peer, err = is_peer_healthy(self, cp.address, cp.port, false)
    if not peer then
      errors[cp.address] = err
    else
      return peer
    end
  end

  return nil, no_host_available_error(errors)
end

function _Cluster:get_next_coordinator()
  if not self.hosts then
    return nil, "no hosts to try, must refresh"
  end

  local errors = {}
  local load_balancing = self.load_balancing_policy

  for _, address in load_balancing(self.shm, self.hosts) do
    local peer, err = is_peer_healthy(self, address, nil, true)
    if not peer then
      errors[address] = err
    else
      return peer
    end
  end

  return nil, no_host_available_error(errors)
end

-- Cluster infos retrieval

local SELECT_PEERS = [[
SELECT peer,data_center,rack,rpc_address FROM system.peers
]]

function _Cluster:refresh()
  local elapsed, err = mutex(self.lock)
  if not elapsed then return nil, err
  elseif elapsed == 0 then
    local coordinator, err = self:get_first_coordinator(self.contact_points)
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

    self.hosts = hosts

    release(self.lock)
  else
    local hosts, err = self:peers()
    if not hosts then return nil, err end
    self.hosts = hosts
  end

  return true
end

-- Queries execution

local function prepare(self, coordinator, query)
  local query_id, err = self:get_prepared(query)
  if err then return nil, err
  elseif query_id == nil then
    local elapsed, err = mutex(self.lock)
    if not elapsed then return nil, err
    elseif elapsed == 0 then
      local res, err = coordinator:prepare(query)
      if not res then return nil, err
      elseif not res.query_id then
        return nil, "could not retrieve query id from response"
      end
      query_id = res.query_id
      local ok, err = self:set_prepared(query, query_id)
      if not ok then return nil, err end
      release(self.lock)
    else
      query_id, err = self:get_prepared(query)
      if err then return nil, err
      elseif query_id == nil then
        return nil, "no query id after preparing query"
      end
    end
  end

  return query_id
end

function _Cluster:execute(query, args, query_options)
  if not self.hosts then
    local ok, err = self:refresh()
    if not ok then return nil, err end
  end

  local coordinator, err = self:get_next_coordinator()
  if not coordinator then return nil, err end

  query_options = query_options or self.query_options

  if query_options.prepared then
    query, err = prepare(self, coordinator, query)
    if not query then return nil, err end
  end

  local res, err, code = coordinator:execute(query, args, query_options)
  coordinator:setkeepalive()
  return res, err, code
end

function _Cluster:shutdown()
  self.shm:flush_all()
  self.shm:flush_expired()
  self.shm_prepared:flush_all()
  self.shm_prepared:flush_expired()
end

return _Cluster
