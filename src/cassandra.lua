local log = require "cassandra.log"
local opts = require "cassandra.options"
local types = require "cassandra.types"
local cache = require "cassandra.cache"
local Object = require "cassandra.classic"
local CONSTS = require "cassandra.constants"
local Errors = require "cassandra.errors"
local Requests = require "cassandra.requests"
local time_utils = require "cassandra.utils.time"
local table_utils = require "cassandra.utils.table"
local string_utils = require "cassandra.utils.string"
local FrameHeader = require "cassandra.types.frame_header"
local FrameReader = require "cassandra.frame_reader"

local table_insert = table.insert
local string_find = string.find
local CQL_Errors = types.ERRORS

--- Host
-- A connection to a single host.
-- Not cluster aware, only maintain a socket to its peer.
-- @section host

local Host = Object:extend()

local function new_socket(self)
  local tcp_sock, sock_type

  if ngx and ngx.get_phase ~= nil and ngx.get_phase() ~= "init" then
    -- lua-nginx-module
    tcp_sock = ngx.socket.tcp
    sock_type = "ngx"
  else
    -- fallback to luasocket
    tcp_sock = require("socket").tcp
    sock_type = "luasocket"
  end

  local socket, err = tcp_sock()
  if not socket then
    error(err)
  end

  self.socket = socket
  self.socket_type = sock_type
end

function Host:new(address, options)
  local host, port = string_utils.split_by_colon(address)
  if not port then port = options.protocol_options.default_port end

  self.host = host
  self.port = port
  self.address = address
  self.protocol_version = CONSTS.DEFAULT_PROTOCOL_VERSION

  self.options = options
  self.reconnection_policy = self.options.policies.reconnection

  new_socket(self)
end

function Host:decrease_version()
  self.protocol_version = self.protocol_version - 1
end

local function send_and_receive(self, request)
  -- Send frame
  local bytes_sent, err = self.socket:send(request:get_full_frame())
  if bytes_sent == nil then
    return nil, err
  end

  -- Receive frame version byte
  local frame_version_byte, err = self.socket:receive(1)
  if frame_version_byte == nil then
    return nil, err
  end

  local n_bytes_to_receive = FrameHeader.size_from_byte(frame_version_byte) - 1

  -- Receive frame header
  local header_bytes, err = self.socket:receive(n_bytes_to_receive)
  if header_bytes == nil then
    return nil, err
  end

  local frameHeader = FrameHeader.from_raw_bytes(frame_version_byte, header_bytes)

  -- Receive frame body
  local body_bytes
  if frameHeader.body_length > 0 then
    body_bytes, err = self.socket:receive(frameHeader.body_length)
    if body_bytes == nil then
      return nil, err
    end
  end

  return FrameReader(frameHeader, body_bytes)
end

function Host:send(request)
  request:set_version(self.protocol_version)

  self:set_timeout(self.options.socket_options.read_timeout)

  local frameReader, err = send_and_receive(self, request)
  if err then
    if err == "timeout" then
      return nil, Errors.TimeoutError(self.address)
    else
      return nil, Errors.SocketError(self.address, err)
    end
  end

  -- result, cql_error
  return frameReader:parse()
end

local function startup(self)
  log.info("Startup request. Trying to use protocol v"..self.protocol_version)

  local startup_req = Requests.StartupRequest()
  return self:send(startup_req)
end

local function change_keyspace(self)
  log.info("Keyspace request. Using keyspace: "..self.options.keyspace)

  local keyspace_req = Requests.KeyspaceRequest(self.options.keyspace)
  return self:send(keyspace_req)
end

function Host:connect()
  log.info("Connecting to "..self.address)

  self:set_timeout(self.options.socket_options.connect_timeout)

  local ok, err = self.socket:connect(self.host, self.port)
  if ok ~= 1 then
    log.info("Could not connect to "..self.address..". Reason: "..err)
    return false, err
  end

  log.info("Session connected to "..self.address)

  if self:get_reused_times() > 0 then
    -- No need for startup request
    return true
  end

  -- Startup request on first connection
  local res, err = startup(self)
  if err then
    log.info("Startup request failed. "..err)
    -- Check for incorrect protocol version
    if err and err.code == frame_reader.errors.PROTOCOL then
      if string_find(err.message, "Invalid or unsupported protocol version:", nil, true) then
        self:close()
        self:decrease_version()
        if self.protocol_version < CONSTS.MIN_PROTOCOL_VERSION then
          log.err("Connection could not find a supported protocol version.")
        else
          log.info("Decreasing protocol version to v"..self.protocol_version)
          return self:connect()
        end
      end
    end

    return false, err
  elseif res.ready then
    log.info("Host at "..self.address.." is ready with protocol v"..self.protocol_version)

    if self.options.keyspace ~= nil then
      local _, err = change_keyspace(self)
      if err then
        return false, err
      end
    end

    return true
  end
end

function Host:set_timeout(t)
  if self.socket_type == "luasocket" then
    -- value is in seconds
    t = t / 1000
  end

  return self.socket:settimeout(t)
end

function Host:get_reused_times()
  if self.socket_type == "ngx" then
    local count, err = self.socket:getreusedtimes()
    if err then
      log.err("Could not get reused times for socket to "..self.address..". "..err)
    end
    return count
  end

  -- luasocket
  return 0
end

function Host:set_keep_alive()
  if self.socket_type == "ngx" then
    local ok, err = self.socket:setkeepalive()
    if err then
      log.err("Could not set keepalive for socket to "..self.address..". "..err)
    end
    return ok
  end

  return true
end

function Host:close()
  log.info("Closing connection to "..self.address..".")
  local res, err = self.socket:close()
  if res ~= 1 then
    log.err("Could not close socket for connection to "..self.address..". "..err)
    return false, err
  else
    return true
  end
end

function Host:set_down()
  log.info("Setting host "..self.address.." as DOWN")
  local host_infos, err = cache.get_host(self.options.shm, self.address)
  if err then
    return false, err
  end

  host_infos.unhealthy_at = time_utils.get_time()
  host_infos.reconnection_delay = self.reconnection_policy.next(self)

  return cache.set_host(self.options.shm, self.address, host_infos)
end

function Host:set_up()
  local host_infos, err = cache.get_host(self.options.shm, self.address)
  if err then
    return false, err
  end

  -- host was previously marked a DOWN
  if host_infos.unhealthy_at ~= 0 then
    log.info("Setting host "..self.address.." as UP")
    host_infos.unhealthy_at = 0
    -- reset schedule for reconnection delay
    self.reconnection_policy.new_schedule(self)
    return cache.set_host(self.options.shm, self.address, host_infos)
  end

  return true
end

function Host:is_up()
  local host_infos, err = cache.get_host(self.options.shm, self.address)
  if err then
    return nil, err
  end

  return host_infos.unhealthy_at == 0
end

function Host:can_be_considered_up()
  local host_infos, err = cache.get_host(self.options.shm, self.address)
  if err then
    return nil, err
  end
  local is_up, err = self:is_up()
  if err then
    return nil, err
  end

  return is_up or (time_utils.get_time() - host_infos.unhealthy_at >= host_infos.reconnection_delay)
end

--- Request Handler
-- @section request_handler

local RequestHandler = {}

function RequestHandler:new(request, options)
  local o = {
    request = request,
    options = options,
    n_retries = 0
  }

  return setmetatable(o, {__index = self})
end

function RequestHandler.get_first_coordinator(hosts)
  local errors = {}
  for _, host in ipairs(hosts) do
    local connected, err = host:connect()
    if not connected then
      errors[host.address] = err
    else
      return host
    end
  end

  return nil, Errors.NoHostAvailableError(errors)
end

function RequestHandler:get_next_coordinator()
  local errors = {}

  local iter = self.options.policies.load_balancing
  local hosts, cache_err = cache.get_hosts(self.options.shm)
  if cache_err then
    return nil, cache_err
  end

  for _, addr in iter(self.options.shm, hosts) do
    local host = Host(addr, self.options)
    local can_host_be_considered_up, cache_err = host:can_be_considered_up()
    if cache_err then
      return nil, cache_err
    elseif can_host_be_considered_up then
      local connected, err = host:connect()
      if connected then
        self.coordinator = host
        return host
      else
        -- bad host, setting DOWN
        local ok, cache_err = host:set_down()
        if not ok then
          return nil, cache_err
        end
        errors[addr] = err
      end
    else
      errors[addr] = "Host considered DOWN"
    end
  end

  return nil, Errors.NoHostAvailableError(errors)
end

function RequestHandler:send()
  local coordinator, err = self:get_next_coordinator()
  if err then
    return nil, err
  end

  log.info("Acquired connection through load balancing policy: "..coordinator.address)

  local result, err = coordinator:send(self.request)

  if coordinator.socket_type == "ngx" then
    coordinator:set_keep_alive()
  else
    coordinator:close()
  end

  if err then
    return self:handle_error(err)
  end

  -- Success! Make sure to re-up node in case it was marked as DOWN
  local ok, cache_err = coordinator:set_up()
  if not ok then
    return nil, cache_err
  end

  return result
end

function RequestHandler:handle_error(err)
  local retry_policy = self.options.policies.retry
  local decision = retry_policy.decisions.throw

  if err.type == "SocketError" then
    -- host seems unhealthy
    local ok, cache_err = self.coordinator:set_down()
    if not ok then
      return nil, cache_err
    end
    -- always retry, another node will be picked
    return self:retry()
  elseif err.type == "TimeoutError" then
    if self.options.query_options.retry_on_timeout then
      return self:retry()
    end
  elseif err.type == "ResponseError" then
    local request_infos = {
      handler = self,
      request = self.request,
      n_retries = self.n_retries
    }
    if err.code == CQL_Errors.OVERLOADED or err.code == CQL_Errors.IS_BOOTSTRAPPING or err.code == CQL_Errors.TRUNCATE_ERROR then
      -- always retry, we will hit another node
      return self:retry()
    elseif err.code == CQL_Errors.UNAVAILABLE_EXCEPTION then
      decision = retry_policy.on_unavailable(request_infos)
    elseif err.code == CQL_Errors.READ_TIMEOUT then
      decision = retry_policy.on_read_timeout(request_infos)
    elseif err.code == CQL_Errors.WRITE_TIMEOUT then
      decision = retry_policy.on_write_timeout(request_infos)
    elseif err.code == CQL_Errors.UNPREPARED then
      -- re-prepare and retry
    end
  end

  if decision == retry_policy.decisions.retry then
    return self:retry()
  end

  -- this error needs to be reported to the session
  return nil, err
end

function RequestHandler:retry()
  self.n_retries = self.n_retries + 1
  log.info("Retrying request")
  return self:send()
end

--- Session
-- A short-lived session, cluster-aware through the cache.
-- Uses a load balancing policy to select a coordinator on which to perform requests.
-- @section session

local Session = {}

function Session:new(options)
  options = opts.parse_session(options)

  local s = {
    options = options,
    coordinator = nil -- to be determined by load balancing policy
  }

  return setmetatable(s, {__index = self})
end

function Session:execute(query, args, options)
  local q_options = table_utils.deep_copy(self.options)
  q_options.query_options = table_utils.extend_table(q_options.query_options, options)

  local query_request = Requests.QueryRequest(query, args, options)
  local request_handler = RequestHandler:new(query_request, q_options)
  return request_handler:send()
end

function Session:set_keyspace(keyspace)
  self.options.keyspace = keyspace
end

function Session:close()
  if self.coordinator ~= nil then
    return self.coordinator:close()
  end
end

--- Cassandra
-- @section cassandra

local Cassandra = {
  _VERSION = "0.4.0"
}

function Cassandra.spawn_session(options)
  return Session:new(options)
end

local SELECT_PEERS_QUERY = "SELECT peer,data_center,rack,rpc_address,release_version FROM system.peers"
local SELECT_LOCAL_QUERY = "SELECT data_center,rack,rpc_address,release_version FROM system.local WHERE key='local'"

--- Retrieve cluster informations form a connected contact_point
function Cassandra.refresh_hosts(contact_points_hosts, options)
  log.info("Refreshing local and peers info")

  local coordinator, err = RequestHandler.get_first_coordinator(contact_points_hosts)
  if err then
    return false, err
  end

  local local_query = Requests.QueryRequest(SELECT_LOCAL_QUERY)
  local peers_query = Requests.QueryRequest(SELECT_PEERS_QUERY)
  local hosts = {}

  local rows, err = coordinator:send(local_query)
  if err then
    return false, err
  end
  local row = rows[1]
  local address = options.policies.address_resolution(row["rpc_address"])
  local local_host = {
    datacenter = row["data_center"],
    rack = row["rack"],
    cassandra_version = row["release_version"],
    protocol_versiom = row["native_protocol_version"],
    unhealthy_at = 0,
    reconnection_delay = 0
  }
  hosts[address] = local_host
  log.info("Local info retrieved")

  rows, err = coordinator:send(peers_query)
  if err then
    return false, err
  end

  for _, row in ipairs(rows) do
    address = options.policies.address_resolution(row["rpc_address"])
    log.info("Adding host "..address)
    hosts[address] = {
      datacenter = row["data_center"],
      rack = row["rack"],
      cassandra_version = row["release_version"],
      protocol_version = local_host.native_protocol_version,
      unhealthy_at = 0,
      reconnection_delay = 0
    }
  end
  log.info("Peers info retrieved")

  coordinator:close()

  -- Store cluster mapping for future sessions
  local addresses = {}
  for addr, host in pairs(hosts) do
    table_insert(addresses, addr)
    local ok, cache_err = cache.set_host(options.shm, addr, host)
    if not ok then
      return false, cache_err
    end
  end

  return cache.set_hosts(options.shm, addresses)
end

--- Retrieve cluster informations and store them in ngx.shared.DICT
function Cassandra.spawn_cluster(options)
  options = opts.parse_cluster(options)

  local contact_points_hosts = {}
  for _, contact_point in ipairs(options.contact_points) do
    table_insert(contact_points_hosts, Host(contact_point, options))
  end

  return Cassandra.refresh_hosts(contact_points_hosts, options)
end

--- CQL types inferers
-- @section

local CQL_TYPES = types.cql_types

local types_mt = {}

function types_mt:__index(key)
  if CQL_TYPES[key] ~= nil then
    return function(value)
      return {value = value, type_id = CQL_TYPES[key]}
    end
  end

  return rawget(self, key)
end

Cassandra.types = setmetatable({}, types_mt)

Cassandra.consistencies = types.consistencies

return Cassandra
