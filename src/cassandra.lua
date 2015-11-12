local Object = require "cassandra.classic"
local CONSTS = require "cassandra.constants"
local Errors = require "cassandra.errors"
local Requests = require "cassandra.requests"
local cache = require "cassandra.cache"
local frame_header = require "cassandra.types.frame_header"
local frame_reader = require "cassandra.frame_reader"
local opts = require "cassandra.options"
local string_utils = require "cassandra.utils.string"
local log = require "cassandra.log"

local table_insert = table.insert
local string_find = string.find

local FrameReader = frame_reader.FrameReader
local FrameHeader = frame_header.FrameHeader

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
  self.address = host..":"..port
  self.protocol_version = CONSTS.DEFAULT_PROTOCOL_VERSION

  self.options = options

  new_socket(self)
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

  --self:set_timeout(self.socket_options.read_timeout)

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

function Host:connect()
  log.info("Connecting to "..self.address)

  local ok, err = self.socket:connect(self.host, self.port)
  if ok ~= 1 then
    log.info("Could not connect to "..self.address..". Reason: "..err)
    return false, err
  end

  log.info("Session connected to "..self.address)

  if self:get_reused_times() > 0 then
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
    return true
  end
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

--- Request handler
-- @section request_handler

local RequestHandler = Object:extend()

function RequestHandler.get_first_host(hosts)
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

--- Session
-- An expandable session, cluster-aware through the cache.
-- Uses a load balancing policy to select nodes on which to perform requests.
-- @section session

local Session = {}

function Session:new(options)
  options = opts.parse_session(options)

  local s = {
    options = options
  }

  return setmetatable(s, {__index = self})
end

function Session:get_next_connection()
  local errors = {}

  local iter = self.options.policies.load_balancing
  local hosts, err = cache.get_hosts(self.options.shm)
  if err then
    return nil, err
  end

  for _, addr in iter(self.options.shm, hosts) do
    local can_host_be_considered_up, err = cache.can_host_be_considered_up(self.options.shm, addr)
    if err then
      return nil, err
    end
    if can_host_be_considered_up then
      local host = Host(addr, self.options)
      local connected, err = host:connect()
      if connected then
        return host
      else
        errors[addr] = err
      end
    else
      errors[addr] = "Host considered DOWN"
    end
  end

  return nil, Errors.NoHostAvailableError(errors)
end

function Session:execute(query)
  local host, err = self:get_next_connection()
  if err then
    return nil, err
  end

  log.info("Acquired connection through load balancing policy: "..host.address)

  local query_request = Requests.QueryRequest(query)
  local result, err = host:send(query_request)
  if err then
    return nil, err
  end

  -- Success! Make sure to re-up node in case it was marked as DOWN
  local ok, err = cache.set_host_up(self.options.shm, host.host)
  if err then
    return nil, err
  end

  if host.socket_type == "ngx" then
    host:set_keep_alive()
  else
    host:close()
  end

  return result
end

function Session:handle_error(err)
  if err.type == "SocketError" then
    -- host seems unhealthy
    self.host:set_down()
    -- always retry
  elseif err.type == "TimeoutError" then
    -- on timeout
  elseif err.type == "ResponseError" then
    if err.code == CQL_Errors.OVERLOADED or err.code == CQL_Errors.IS_BOOTSTRAPPING or err.code == CQL_Errors.TRUNCATE_ERROR then
      -- always retry
    elseif err.code == CQL_Errors.UNAVAILABLE_EXCEPTION then
      -- make retry decision based on retry_policy on_unavailable
    elseif err.code == CQL_Errors.READ_TIMEOUT then
      -- make retry decision based on retry_policy read_timeout
    elseif err.code == CQL_Errors.WRITE_TIMEOUT then
      -- make retry decision based on retry_policy write_timeout
    end
  end

  -- this error needs to be reported to the client
  return nil, err
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

  local host, err = RequestHandler.get_first_host(contact_points_hosts)
  if err then
    return false, err
  end

  local local_query = Requests.QueryRequest(SELECT_LOCAL_QUERY)
  local peers_query = Requests.QueryRequest(SELECT_PEERS_QUERY)
  local hosts = {}

  local rows, err = host:send(local_query)
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
    reconnection_delay = 5
  }
  hosts[address] = local_host
  log.info("Local info retrieved")

  rows, err = host:send(peers_query)
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
      reconnection_delay = 5
    }
  end
  log.info("Peers info retrieved")

  -- Store cluster mapping for future sessions
  local addresses = {}
  for addr, host in pairs(hosts) do
    table_insert(addresses, addr)
    local ok, err = cache.set_host(options.shm, addr, host)
    if err then
      return false, err
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

return Cassandra
