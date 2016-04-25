local resty_lock = require 'resty.lock'
local cassandra = require 'cassandra'
local cql = require 'cassandra.cql'
local ffi = require 'ffi'

local cql_errors = cql.errors
local ffi_cast = ffi.cast
local ffi_str = ffi.string
local requests = cql.requests
local tonumber = tonumber
local concat = table.concat
local shared = ngx.shared
local pairs = pairs
local sub = string.sub
local now = ngx.now
local type = type
local log = ngx.log
local ERR = ngx.ERR
local DEBUG = ngx.DEBUG
local NOTICE = ngx.NOTICE

local _rec_key = 'host:rec:'
local _prepared_key = 'prepared:id:'

ffi.cdef [[
    struct peer_rec {
        uint64_t      reconn_delay;
        uint64_t      unhealthy_at;
    };
]]
local rec_peer_const = ffi.typeof('const struct peer_rec*')
local rec_peer_size = ffi.sizeof('struct peer_rec')
local rec_peer_cdata = ffi.new('struct peer_rec')

local function get_now()
  return now() * 1000
end

-----------------------------
-- Hosts health stored in shm
-----------------------------

local function set_peer(self, host, up, reconn_delay, unhealthy_at)
  -- status
  local ok, err = self.shm:set(host, up)
  if not ok then
    return nil, 'could not set host status in shm: '..err
  end

  -- health details
  rec_peer_cdata.reconn_delay = reconn_delay
  rec_peer_cdata.unhealthy_at = unhealthy_at
  ok, err = self.shm:set(_rec_key..host, ffi_str(rec_peer_cdata, rec_peer_size))
  if not ok then
    return nil, 'could not set host info in shm: '..err
  end

  return true
end

local function get_peer(self, host, status)
  local v, err = self.shm:get(_rec_key .. host)
  if err then
    return nil, 'could not get host details in shm: '..err
  elseif type(v) ~= 'string' or #v ~= rec_peer_size then
    return nil, 'corrupted shm'
  end

  if status == nil then
    status, err = self.shm:get(host)
    if err then return nil, 'could not get host status in shm: '..err end
  end

  local peer_rec = ffi_cast(rec_peer_const, v)

  return {
    up = status,
    host = host,
    reconn_delay = tonumber(peer_rec.reconn_delay),
    unhealthy_at = tonumber(peer_rec.unhealthy_at)
  }
end

local function get_peers(self)
  local peers = {}
  local keys = self.shm:get_keys() -- 1024 keys
  -- we shall have a relatively small number of keys, but in any case this
  -- function is not to be called in hot paths anyways.
  for i = 1, #keys do
    if sub(keys[i], 1, #_rec_key) == _rec_key then
      local host = sub(keys[i], #_rec_key + 1)
      local peer, err = get_peer(self, host)
      if not peer then return nil, err end
      peers[#peers+1] = peer
    end
  end

  if #peers > 0 then
    return peers
  end
end


local function set_peer_down(self, host)
  return set_peer(self, host, false, self.reconn_policy:next_delay(host), get_now())
end

local function set_peer_up(self, host)
  self.reconn_policy:reset(host)
  return set_peer(self, host, true, 0, 0)
end

local function can_try_peer(self, host)
  local up, err = self.shm:get(host)
  if up then return up
  elseif err then return nil, err
  else
    -- reconnection policy steps in before making a decision
    local peer_rec, err = get_peer(self, host, up)
    if not peer_rec then return nil, err end
    return get_now() - peer_rec.unhealthy_at >= peer_rec.reconn_delay, nil, true
  end
end

----------------------------
-- utils
----------------------------

local function spawn_peer(host, port, opts)
  opts = opts or {}
  opts.host = host
  opts.port = port
  return cassandra.new(opts)
end

local function check_peer_health(self, host, retry)
  -- TODO: maybe we ought not to care about the keyspace set in
  -- peers_opts when simply checking the connction to a node.
  local peer, err = spawn_peer(host, self.default_port, self.peers_opts)
  if not peer then return nil, err
  else
    peer:settimeout(self.timeout_connect)
    local ok, err, maybe_down = peer:connect()
    if ok then
      -- host is healthy
      if retry then
        -- node seems healthy after being down, back up!
        local ok, err = set_peer_up(self, host)
        if not ok then return nil, 'error setting host back up: '..err end
      end

      return peer
    elseif maybe_down then
      -- host is not (or still not) responsive
      local ok, shm_err = set_peer_down(self, host)
      if not ok then return nil, 'error setting host down: '..shm_err end

      return nil, 'host seems unhealthy, considering it down ('..err..')'
    else
      return nil, err
    end
  end
end

-----------
-- Cluster
-----------

local _Cluster = {}
_Cluster.__index = _Cluster

function _Cluster.new(opts)
  opts = opts or {}
  if type(opts) ~= 'table' then
    return nil, 'opts must be a table'
  end

  local peers_opts = {}
  local dict_name = opts.shm or 'cassandra'
  if type(dict_name) ~= 'string' then
    return nil, 'shm must be a string'
  elseif not shared[dict_name] then
    return nil, 'no shared dict '..dict_name
  end

  for k, v in pairs(opts) do
    if k == 'keyspace' then
      if type(v) ~= 'string' then
        return nil, 'keyspace must be a string'
      end
      peers_opts.keyspace = v
    elseif k == 'ssl' then
      if type(v) ~= 'boolean' then
        return nil, 'ssl must be a boolean'
      end
      peers_opts.ssl = v
    elseif k == 'verify' then
      if type(v) ~= 'boolean' then
        return nil, 'verify must be a boolean'
      end
      peers_opts.verify = v
    elseif k == 'auth' then
      if type(v) ~= 'table' then
        return nil, 'auth seems not to be an auth provider'
      end
      peers_opts.auth = v
    elseif k == 'default_port' then
      if type(v) ~= 'number' then
        return nil, 'default_port must be a number'
      end
    elseif k == 'contact_points' then
      if type(v) ~= 'table' then
        return nil, 'contact_points must be a table'
      end
    elseif k == 'read_timeout' then
      if type(v) ~= 'number' then
        return nil, 'read_timeout must be a number'
      end
    elseif k == 'connect_timeout' then
      if type(v) ~= 'number' then
        return nil, 'connect_timeout must be a number'
      end
    elseif k == 'max_schema_consensus_wait' then
      if type(v) ~= 'number' then
        return nil, 'max_schema_consensus_wait must be a number'
      end
    elseif k == 'retry_on_timeout' then
      if type(v) ~= 'boolean' then
        return nil, 'retry_on_timeout must be a boolean'
      end
    end
  end

  return setmetatable({
    shm = shared[dict_name],
    dict_name = dict_name,
    prepared_ids = {},
    peers_opts = peers_opts,
    default_port = opts.default_port or 9042,
    contact_points = opts.contact_points or {'127.0.0.1'},
    timeout_read = opts.timeout_read or 2000,
    timeout_connect = opts.timeout_connect or 1000,
    retry_on_timeout = opts.retry_on_timeout == nil and true or opts.retry_on_timeout,
    max_schema_consensus_wait = opts.max_schema_consensus_wait or 10000,

    lb_policy = opts.lb_policy
                or require('resty.cassandra.policies.lb.rr').new(),
    reconn_policy = opts.reconn_policy
                or require('resty.cassandra.policies.reconnection.exp').new(1000, 60000)
  }, _Cluster)
end

local function no_host_available_error(errors)
  local buf = {'all hosts tried for query failed'}
  for address, err in pairs(errors) do
    buf[#buf+1] = address..': '..err
  end
  return concat(buf, '. ')
end

local function first_coordinator(self)
  local errors = {}
  local cp = self.contact_points

  for i = 1, #cp do
    local peer, err = check_peer_health(self, cp[i])
    if not peer then
      errors[cp[i]] = err
    else
      return peer
    end
  end

  return nil, no_host_available_error(errors)
end

local function next_coordinator(self)
  local errors = {}

  for _, peer_rec in self.lb_policy:iter() do
    local ok, err, retry = can_try_peer(self, peer_rec.host)
    if ok then
      local peer, err = check_peer_health(self, peer_rec.host, retry)
      if peer then
        return peer
      else
        errors[peer_rec.host] = err
      end
    elseif err then
      return nil, err
    else
      errors[peer_rec.host] = 'host still considered down'
    end
  end

  return nil, no_host_available_error(errors)
end

function _Cluster:refresh()
  local old_peers, err = get_peers(self)
  if err then return nil, err
  elseif old_peers then
    -- we need to first flush the existing peers from the shm,
    -- so that our lock can work properly. we keep old peers in
    -- our local for later.
    for i = 1, #old_peers do
      local host = old_peers[i].host
      old_peers[host] = old_peers[i] -- alias as a hash
      self.shm:delete(_rec_key .. host)
      self.shm:delete(host)
    end
  else
    old_peers = {} -- empty shm
  end

  local lock = resty_lock:new(self.dict_name)
  local elapsed, err = lock:lock('refresh')
  if not elapsed then return nil, 'failed to acquire lock: '..err end

  -- did someone else got the hosts?
  local peers, err = get_peers(self)
  if err then return nil, err
  elseif not peers then
    -- we are the first ones to get there
    local coordinator, err = first_coordinator(self)
    if not coordinator then return nil, err end

    local rows, err = coordinator:execute [[
      SELECT peer,data_center,rpc_address FROM system.peers
    ]]
    if not rows then return nil, err end

    coordinator:setkeepalive()

    rows[#rows+1] = {rpc_address = coordinator.host} -- local host

    for i = 1, #rows do
      local host = rows[i].rpc_address
      local old_peer = old_peers[host]
      local reconn_delay, unhealthy_at = 0, 0
      local up = true
      if old_peer then
        up = old_peer.up
        reconn_delay = old_peer.reconn_delay
        unhealthy_at = old_peer.unhealthy_at
      end

      local ok, err = set_peer(self, host, up, reconn_delay, unhealthy_at)
      if not ok then return nil, err end
    end

    peers, err = get_peers(self)
    if err then return nil, err end
  end

  local ok, err = lock:unlock()
  if not ok then return nil, 'failed to unlock: '..err end

  self.lb_policy:init(peers)
  self.init = true
  return true
end

--------------------
-- queries execution
--------------------

local get_request_opts = cassandra.get_request_opts
local query_req = requests.query.new
local batch_req = requests.batch.new
local prep_req = requests.execute_prepared.new

local function check_schema_consensus(coordinator)
  local local_res, err = coordinator:execute('SELECT schema_version FROM system.local')
  if not local_res then return nil, err end

  local peers_res, err = coordinator:execute('SELECT schema_version FROM system.peers')
  if not peers_res then return nil, err end

  if #peers_res > 0 and #local_res > 0 then
    for i = 1, #peers_res do
      if peers_res[i].schema_version ~= local_res[1].schema_version then
        return nil
      end
    end
  end

  return local_res[1].schema_version
end

local function wait_schema_consensus(self, coordinator)
  local peers, err = get_peers(self)
  if err then return nil, err
  elseif not peers then return nil, 'no peers in shm'
  elseif #peers < 2 then return true end

  local ok, err, tdiff
  local tstart = get_now()

  repeat
    --ngx.sleep(0.5)
    ok, err = check_schema_consensus(coordinator)
    tdiff = get_now() - tstart
  until ok or err or tdiff >= self.max_schema_consensus_wait

  if ok then
    return ok
  elseif err then
    return nil, err
  else
    return nil, 'timeout'
  end
end

local function prepare(self, coordinator, query)
  log(DEBUG, 'preparing ', query, ' on host ', coordinator.host)
  -- we are the ones preparing the query
  local res, err = coordinator:prepare(query)
  if not res then return nil, 'could not prepare query: '..err end
  return res.query_id
end

local function get_or_prepare(self, coordinator, query)
  -- worker memory check
  local query_id = self.prepared_ids[query]
  if not query_id then
    -- worker cache miss
    -- shm cache?
    local shm = self.shm
    local key = _prepared_key .. query
    local err
    query_id, err = shm:get(key)
    if err then return nil, 'could not get query id from shm:'..err
    elseif not query_id then
      -- shm cache miss
      -- query not prepared yet, must prepare in mutex
      local lock = resty_lock:new(self.dict_name)
      local elapsed, err = lock:lock('prepare:' .. query)
      if not elapsed then return nil, 'failed to acquire lock: '..err end

      -- someone else prepared query?
      query_id, err = shm:get(key)
      if err then return nil, 'could not get query id from shm:'..err
      elseif not query_id then
        query_id, err = prepare(self, coordinator, query)
        if not query_id then return nil, err end

        local ok, err = shm:set(key, query_id)
        if not ok then return nil, 'could not set query id in shm: '..err end
      end

      local ok, err = lock:unlock()
      if not ok then return nil, 'failed to unlock: '..err end
    end

    -- set worker cache
    self.prepared_ids[query] = query_id
  end

  return query_id
end

local inner_send

local function prepare_and_retry(self, query, args, opts, coordinator)
  local query_id, err = prepare(self, coordinator, query)
  if not query_id then return nil, err end

  local req = prep_req(query_id, args, opts)

  return inner_send(self, coordinator, req, query, args, opts)
end

local function handle_error(self, err, cql_code, coordinator, query, args, opts)
  if cql_code and cql_code == cql_errors.UNPREPARED then
    log(NOTICE, query, ' not prepared on host ', coordinator.host, ',',
                ' preparing and retrying')
    return prepare_and_retry(self, query, args, opts, coordinator)
  end

  coordinator:setkeepalive()

  return nil, err, cql_code
end

inner_send = function(self, coordinator, request, query, args, opts)
  local res, err, cql_code = coordinator:send(request)
  if not res then
    return handle_error(self, err, cql_code, coordinator, query, args, opts)
  elseif res.type == 'SCHEMA_CHANGE' then
    local schema_version, err = wait_schema_consensus(self, coordinator)
    if not schema_version then
      return nil, 'could not check schema consensus: '..err
    end

    res.schema_version = schema_version
  end

  coordinator:setkeepalive()

  return res
end

------------
-- execute()
------------

function _Cluster:execute(query, args, options)
  if not self.init then
    local ok, err = self:refresh()
    if not ok then return nil, 'could not refresh cluster: '..err end
  end

  local coordinator, err = next_coordinator(self)
  if not coordinator then return nil, err end

  local request
  local opts = get_request_opts(options)

  if opts.prepared then
    local query_id, err = get_or_prepare(self, coordinator, query)
    if not query_id then return nil, err end

    request = prep_req(query_id, args, opts)
  else
    request = query_req(query, args, opts)
  end

  return inner_send(self, coordinator, request, query, args, opts)
end

function _Cluster:batch(queries, options)
  if not self.init then
    local ok, err = self:refresh()
    if not ok then return nil, 'could not refresh cluster: '..err end
  end

  local coordinator, err = next_coordinator(self)
  if not coordinator then return nil, err end

  local request
  local opts = get_request_opts(options)

  if opts.prepared then
    local queries_ids = {}
    for i = 1, #queries do
      local query, args = queries[i]
      if type(query) == 'table' then
        query, args = query[1], query[2]
      end
      print(query)
      local query_id, err = get_or_prepare(self, coordinator, query)
      if not query_id then return nil, err end
      queries_ids[i] = {query_id, args}
    end
    request = batch_req(queries_ids, opts)
  else
    request = batch_req(queries, opts)
  end

  return inner_send(self, coordinator, request, queries, args, opts)
end

_Cluster.set_peer = set_peer
_Cluster.get_peer = get_peer
_Cluster.get_peers = get_peers
_Cluster.set_peer_up = set_peer_up
_Cluster.set_peer_down = set_peer_down
_Cluster.can_try_peer = can_try_peer
_Cluster.next_coordinator = next_coordinator
_Cluster.get_or_prepare = get_or_prepare

return _Cluster
