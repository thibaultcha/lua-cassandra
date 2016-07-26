--- Cassandra cluster client module.
-- Cluster module for OpenResty.
-- @module resty.cassandra.cluster
-- @author thibaultcha
-- @release 1.0.0

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
local WARN = ngx.WARN
local DEBUG = ngx.DEBUG
local NOTICE = ngx.NOTICE

local _log_prefix = '[lua-cassandra] '
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
  log(WARN, _log_prefix, 'setting host at ', host.coordinator, ' DOWN')
  return set_peer(self, host, false, self.reconn_policy:next_delay(host), get_now())
end

local function set_peer_up(self, host)
  log(NOTICE, _log_prefix, 'setting host at ', host.coordinator, ' UP')
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

      peer:settimeout(self.timeout_read)

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

local _Cluster = {
  _VERSION = '1.0.0'
}

_Cluster.__index = _Cluster

--- New cluster options.
-- Options taken by `new` upon cluster creation.
-- @field shm Name of the lua_shared_dict to use for this cluster's
-- information. (`string`, default: `cassandra`)
-- @field contact_points Array of addresses for this cluster's
-- contact points. (`table`, default: `{"127.0.0.1"}`)
-- @field default_port The port on which all nodes from the cluster are
-- listening on. (`number`, default: `9042`)
-- @field keyspace Keyspace to use for this cluster. (`string`, optional)
-- @field connect_timeout The timeout value when connecing to a node, in ms.
-- (`number`, default: `1000`)
-- @field read_timeout The timeout value when reading from a node, in ms.
-- (`number`, default: `2000`)
-- @field retry_on_timeout Specifies if the request should be retried on the
-- next coordinator (as per the load balancing policy)
-- if it timed out. (`boolean`, default: `true`)
-- @field max_schema_consensus_wait Maximum waiting time allowed when executing
-- DDL queries before timing out, in ms.
-- (`number`, default: `10000`)
-- @field lb_policy A load balancing policy created from one of the modules
-- under `resty.cassandra.policies.lb.*`.
-- (`lb policy`, default: `lb.rr` round robin)
-- @field reconn_policy A reconnection policy created from one of the modules
-- under `resty.cassandra.policies.reconnection.*`.
-- (`reconn policy`, default: `reconnection.exp` (exponential)
-- 1000ms base, 60000ms max)
-- @field retry_policy A retry policy created from one of the modules
-- under `resty.cassandra.policies.retry.*`.
-- (`retry policy`, default: `retry.simple`, 3 retries)
-- @field ssl Determines if the created cluster should connect using SSL.
-- (`boolean`, default: `false`)
-- @field verify Enable server certificate validation if `ssl` is enabled.
-- (`boolean`, default: `false`)
-- @field auth Authentication handler, created from the
-- `cassandra.auth_providers` table. (optional)
-- @table `cluster_options`

--- Create a new Cluster client.
-- Takes a table of `cluster_options`. Does not connect automatically.
-- On the first request to the cluster, the module will attempt to connect to
-- one of the specified `contact_points`, and retrieve the full list of nodes
-- belonging to this cluster. Once this list retrieved, the load balancing
-- policy will start selecting nodes to act as coordinators for the future
-- requests.
--
-- @usage
-- local Cluster = require "resty.cassandra.cluster"
-- local cluster = Cluster.new {
--   shm = "cassandra_shared_dict",
--   contact_points = {"10.0.0.1", "10.0.0.2"},
--   keyspace = "my_keyspace",
--   default_port = 9042,
--   connect_timeout = 3000
-- }
--
-- @param[type=table] opts Options for the created cluster client.
-- @treturn table `cluster`: A table holding clustering operations capabilities
-- or nil if failure.
-- @treturn string `err`: String describing the error if failure.
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
                or require('resty.cassandra.policies.reconnection.exp').new(1000, 60000),
    retry_policy = opts.retry_policy
                or require('resty.cassandra.policies.retry.simple').new(3)
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
        log(DEBUG, _log_prefix, 'load balancing policy chose host at ',  peer.host)
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

--- Refresh the list of nodes in the cluster.
-- Queries one of the specified `contact_points` to retrieve the list of
-- available nodes in the cluster, and update the configured policies.
-- This method is automatically called upon the first query made to the
-- cluster (from `execute`, `batch` or `iterate`), but needs to be manually
-- called if further updates are required.
-- @treturn boolean `ok`: `true` if success, `nil` if failure.
-- @treturn string `err`: String describing the error if failure.
function _Cluster:refresh()
  local old_peers, err = get_peers(self)
  if err then return nil, err
  elseif old_peers then
    -- we first need to flush the existing peers from the shm,
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
  log(DEBUG, _log_prefix, 'preparing ', query, ' on host ', coordinator.host)
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

local send_request

function _Cluster:send_retry(request)
  local coordinator, err = next_coordinator(self)
  if not coordinator then return nil, err end

  log(NOTICE, _log_prefix, 'retrying request on host at ', coordinator.host)

  request.retries = request.retries + 1

  return send_request(self, coordinator, request)
end

local function prepare_and_retry(self, coordinator, request)
  if request.queries then
    -- prepared batch
    log(NOTICE, _log_prefix, 'some requests from this batch were not prepared on host ',
                 coordinator.host, ', preparing and retrying')
    for i = 1, #request.queries do
      local query_id, err = prepare(self, coordinator, request.queries[i][1])
      if not query_id then return nil, err end
      request.queries[i][3] = query_id
    end
  else
    -- prepared query
    log(NOTICE, _log_prefix, request.query, ' was not prepared on host ',
                coordinator.host, ', preparing and retrying')
    local query_id, err = prepare(self, coordinator, request.query)
    if not query_id then return nil, err end
    request.query_id = query_id
  end

  return send_request(self, coordinator, request)
end

local function handle_error(self, err, cql_code, coordinator, request)
  if cql_code and cql_code == cql_errors.UNPREPARED then
    return prepare_and_retry(self, coordinator, request)
  end

  -- failure, need to try another coordinator
  coordinator:setkeepalive()

  if cql_code then
    local retry
    if cql_code == cql_errors.OVERLOADED or
       cql_code == cql_errors.IS_BOOTSTRAPPING or
       cql_code == cql_errors.TRUNCATE_ERROR then
      retry = true
    elseif cql_code == cql_errors.UNAVAILABLE_EXCEPTION then
      retry = self.retry_policy:on_unavailable(request)
    elseif cql_code == cql_errors.READ_TIMEOUT then
      retry = self.retry_policy:on_read_timeout(request)
    elseif cql_code == cql_errors.WRITE_TIMEOUT then
      retry = self.retry_policy:on_write_timeout(request)
    end

    if retry then
      return self:send_retry(request)
    end
  elseif err == 'timeout' then
    if self.retry_on_timeout then
      return self:send_retry(request)
    end
  else
    -- host seems down?
    local ok, err = set_peer_down(self, coordinator.host)
    if not ok then return nil, err end

    return self:send_retry(request)
  end

  return nil, err, cql_code
end

send_request = function(self, coordinator, request)
  local res, err, cql_code = coordinator:send(request)
  if not res then
    return handle_error(self, err, cql_code, coordinator, request)
  end

  if res.type == 'SCHEMA_CHANGE' then
    local schema_version, err = wait_schema_consensus(self, coordinator)
    if not schema_version then
      coordinator:setkeepalive()
      return nil, 'could not check schema consensus: '..err
    end

    res.schema_version = schema_version
  end

  coordinator:setkeepalive()

  return res
end

do
  local get_request_opts = cassandra.get_request_opts
  local page_iterator = cassandra.page_iterator
  local query_req = requests.query.new
  local batch_req = requests.batch.new
  local prep_req = requests.execute_prepared.new

  --- Execute a query.
  -- Sends a request to the coordinator chosen by the configured load
  -- balancing policy. The policy always chooses nodes that are considered
  -- healthy, and eventually reconnects to unhealthy nodes as per the
  -- configured reconnection policy.
  -- Requests that fail because of timeouts can be retried on the next
  -- available node if `retry_on_timeout` is enabled, and failed requests
  -- can be retried as per defined in the configured retry policy.
  --
  -- @usage
  -- local Cluster = require "resty.cassandra.cluster"
  -- local cluster, err = Cluster.new()
  -- if not cluster then
  --   ngx.log(ngx.ERR, "could not create cluster: ", err)
  --   ngx.exit(500)
  -- end
  --
  -- local rows, err = cluster:execute("SELECT * FROM users WHERE age = ?". {
  --   21
  -- }, {
  --   page_size = 100
  -- })
  -- if not rows then
  --   ngx.log(ngx.ERR, "could not retrieve users: ", err)
  --   ngx.exit(500)
  -- end
  --
  -- ngx.say("page size: ", #rows, " next page: ", rows.meta.paging_state)
  --
  -- @param[type=string] query CQL query to execute.
  -- @param[type=table] args (optional) Arguments to bind to the query.
  -- @param[type=table] options (optional) Options from `query_options`
  -- for this query.
  -- @treturn table `res`: Table holding the query result if success, `nil` if failure.
  -- @treturn string `err`: String describing the error if failure.
  -- @treturn number `cql_err`: If a server-side error occurred, the CQL error code.
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
      request = prep_req(query_id, args, opts, query)
    else
      request = query_req(query, args, opts)
    end

    return send_request(self, coordinator, request)
  end

  --- Execute a batch.
  -- Sends a request to execute the given batch. Load balancing, reconnection,
  -- and retry policies act the same as described for `execute`.
  -- @usage
  -- local Cluster = require "resty.cassandra.cluster"
  -- local cluster, err = Cluster.new()
  -- if not cluster then
  --   ngx.log(ngx.ERR, "could not create cluster: ", err)
  --   ngx.exit(500)
  -- end
  --
  -- local res, err = cluster:batch({
  --   {"INSERT INTO things(id, n) VALUES(?, 1)", {123}},
  --   {"UPDATE things SET n = 2 WHERE id = ?", {123}},
  --   {"UPDATE things SET n = 3 WHERE id = ?", {123}}
  -- }, {
  --   logged = false
  -- })
  -- if not res then
  --   ngx.log(ngx.ERR, "could not execute batch: ", err)
  --   ngx.exit(500)
  -- end
  --
  -- @treturn table `res`: Table holding the query result if success, `nil` if failure.
  -- @treturn string `err`: String describing the error if failure.
  -- @treturn number `cql_err`: If a server-side error occurred, the CQL error code.
  function _Cluster:batch(queries_t, options)
    if not self.init then
      local ok, err = self:refresh()
      if not ok then return nil, 'could not refresh cluster: '..err end
    end

    local coordinator, err = next_coordinator(self)
    if not coordinator then return nil, err end

    local opts = get_request_opts(options)

    if opts.prepared then
      for i = 1, #queries_t do
        local query_id, err = get_or_prepare(self, coordinator, queries_t[i][1])
        if not query_id then return nil, err end
        queries_t[i][3] = query_id
      end
    end

    return send_request(self, coordinator, batch_req(queries_t, opts))
  end

  --- Lua iterator for auto-pagination.
  -- Perform auto-pagination for a query when used as a Lua iterator.
  -- Load balancing, reconnection, and retry policies act the same as described
  -- for `execute`.
  --
  -- @usage
  -- local Cluster = require "resty.cassandra.cluster"
  -- local cluster, err = Cluster.new()
  -- if not cluster then
  --   ngx.log(ngx.ERR, "could not create cluster: ", err)
  --   ngx.exit(500)
  -- end
  --
  -- for rows, err, page in cluster:iterate("SELECT * FROM users") do
  --   if err then
  --     ngx.log(ngx.ERR, "could not retrieve page: ", err)
  --     ngx.exit(500)
  --   end
  --   ngx.say("page ", page, " has ", #rows, " rows")
  -- end
  --
  -- @param[type=string] query CQL query to execute.
  -- @param[type=table] args (optional) Arguments to bind to the query.
  -- @param[type=table] options (optional) Options from `query_options`
  -- for this query.
  function _Cluster:iterate(query, args, options)
    return page_iterator(self, query, args, options)
  end
end

_Cluster.set_peer = set_peer
_Cluster.get_peer = get_peer
_Cluster.get_peers = get_peers
_Cluster.set_peer_up = set_peer_up
_Cluster.can_try_peer = can_try_peer
_Cluster.handle_error = handle_error
_Cluster.set_peer_down = set_peer_down
_Cluster.get_or_prepare = get_or_prepare
_Cluster.next_coordinator = next_coordinator

return _Cluster
