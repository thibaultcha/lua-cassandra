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
local type = type
local log = ngx.log
local ERR = ngx.ERR

ffi.cdef [[
    struct peer_rec {
        uint64_t      reconn_delay;
        uint64_t      unhealthy_at;
    };
]]
local rec_peer_const = ffi.typeof('const struct peer_rec*')
local rec_peer_size = ffi.sizeof('struct peer_rec')
local rec_peer_cdata = ffi.new('struct peer_rec')

local function lock(self, k)
  local elapsed, err = self.lock:lock(k)
  if err then
    log(ERR, 'could not acquire lock: ', err)
  end
  return elapsed
end

local function release(self)
  local ok, err = self.lock:unlock()
  if not ok then
    log(ERR, 'could not release lock: ', err)
  end
end

local function spawn_peer(host, port)
  return cassandra.new {
    host = host,
    port = port
  }
end

local function set_peer(self, host, peer_rec)
  rec_peer_cdata.reconn_delay = peer_rec.reconn_delay
  rec_peer_cdata.unhealthy_at = peer_rec.unhealthy_at
  return self.shm:set(host, ffi_str(rec_peer_cdata, rec_peer_size))
end

local function get_peer(self, host)
  local v = self.shm:get(host)
  if type(v) ~= 'string' or #v ~= rec_peer_size then
    return nil, 'corrupted shm'
  end

  rec_peer_cdata = ffi_cast(rec_peer_const, v)
  return {
    host = host,
    reconn_delay = tonumber(rec_peer_cdata.reconn_delay),
    unhealthy_at = tonumber(rec_peer_cdata.unhealthy_at)
  }
end

local function get_peers(self)
  local peers = {}
  local keys = self.shm:get_keys() -- 1024 keys
  for i = 1, #keys do
    local peer, err = get_peer(self, keys[i])
    if not peer then return nil, err end
    peers[#peers+1] = peer
  end
  return peers
end

local function set_peer_down(host)

end

local function set_peer_up(host)

end

local _Cluster = {}
_Cluster.__index = _Cluster

function _Cluster.new(opts)
  opts = opts or {}
  if type(opts) ~= 'table' then
    return nil, 'opts must be a table'
  end

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
    peers = {},
    shm = shared[dict_name],
    lock = resty_lock:new(dict_name),
    keyspace = opts.keyspace,
    default_port = opts.default_port or 9042,
    contact_points = opts.contact_points or {'127.0.0.1'},
    timeout_read = opts.timeout_read or 2000,
    timeout_connect = opts.timeout_connect or 1000,
    retry_on_timeout = opts.retry_on_timeout ~= nil and true or opts.retry_on_timeout,
    max_schema_consensus_wait = opts.max_schema_consensus_wait or 10000
  }, _Cluster)
end

local function no_host_available_error(errors)
  local buf = {"all hosts tried for query failed."}
  for address, err in pairs(errors) do
    buf[#buf+1] = address..": "..err
  end
  return concat(buf, " ")
end

local function first_coordinator(self)
  local errors = {}
  local cp = self.contact_points

  for i = 1, #cp do
    local peer, err = spawn_peer(cp[i], self.default_port)
    if not peer then
      errors[cp[i]] = err
    else
      peer:settimeout(self.timeout_connect)
      local ok, err, maybe_down = peer:connect()
      if ok then
        return peer
      elseif maybe_down then
        errors[cp[i]] = 'host seems unhealthy: '..err
      else
        errors[cp[i]] = err -- hard error
      end
    end
  end

  return nil, no_host_available_error(errors)
end

local function get_next_coordinator(self)

end

local _select_peers = [[
SELECT peer,data_center,rack,rpc_address FROM system.peers
]]

function _Cluster:refresh()
  local elapsed = lock(self, 'refresh')
  if elapsed and elapsed == 0 then
    local coordinator, err = first_coordinator(self)
    if not coordinator then return nil, err end

    local rows, err = coordinator:execute(_select_peers)
    if not rows then return nil, err end
    coordinator:setkeepalive()

    self.shm:flush_all()

    rows[#rows+1] = {rpc_address = coordinator.host}

    for i = 1, #rows do
      set_peer(self, rows[i].rpc_address, {
        reconn_delay = 0,
        unhealthy_at = 0
      })
    end

    release(self)
  end

  local peers, err = get_peers(self)
  if not peers then return nil, err end

  self.peers = peers

  return true
end

_Cluster.get_shm_peers = get_peers
_Cluster.set_shm_peer = set_peer

return _Cluster
