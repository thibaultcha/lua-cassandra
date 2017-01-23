--- Cassandra client library for Lua and ngx_lua (OpenResty).
-- Compatible with Cassandra 2.0+, Lua 5.1, 5.2, 5.3, LuaJIT, OpenResty.
--
-- Support for binary protocols v2 and v3.
--
-- @module cassandra
-- @author thibaultcha
-- @release 0.5.0

-- @TODO
-- flush dicts on shutdown
-- tracing

local log = require "cassandra.log"
local opts = require "cassandra.options"
local types = require "cassandra.types"
local cache = require "cassandra.cache"
local socket = require "cassandra.socket"
local Errors = require "cassandra.errors"
local Requests = require "cassandra.requests"
local time_utils = require "cassandra.utils.time"
local table_utils = require "cassandra.utils.table"
local string_utils = require "cassandra.utils.string"
local FrameHeader = require "cassandra.types.frame_header"
local FrameReader = require "cassandra.frame_reader"

local resty_lock
local status, res = pcall(require, "resty.lock")
if status then
  resty_lock = res
end

local CQL_Errors = types.ERRORS
local string_find = string.find
local table_insert = table.insert
local string_format = string.format
local setmetatable = setmetatable
local ipairs = ipairs
local pairs = pairs

local function lock_mutex(shm, key)
  if resty_lock then
    local lock = resty_lock:new(shm)
    local elapsed, err = lock:lock(key)
    if err then
      err = "Error locking mutex: "..err
    end
    return lock, err, elapsed
  else
    return nil, nil, 0
  end
end

local function unlock_mutex(lock)
  if resty_lock then
    local ok, err = lock:unlock()
    if not ok then
      err = "Error unlocking mutex: "..err
    end
    return err
  end
end

--- Constants

local MIN_PROTOCOL_VERSION = 2
local DEFAULT_PROTOCOL_VERSION = 3

--- Cassandra

local Cassandra = {
  _VERSION = "0.5.5",
  DEFAULT_PROTOCOL_VERSION = DEFAULT_PROTOCOL_VERSION,
  MIN_PROTOCOL_VERSION = MIN_PROTOCOL_VERSION
}

--- Host
-- A connection to a single host.
-- Not cluster aware, only maintain a socket to its peer.

local Host = {}
Host.__index = Host

local function new_socket(self)
  local sock, err = socket.tcp()
  if not sock then
    error("Could not create socket: "..err)
  end

  self.socket = sock
end

function Host:new(address, options)
  local host, port = string_utils.split_by_colon(address)
  if not port then port = options.protocol_options.default_port end

  local h = {
    host = host,
    port = port,
    address = address,
    protocol_version = options.protocol_options.version or DEFAULT_PROTOCOL_VERSION,
    options = options,
    reconnection_policy = options.policies.reconnection
  }

  return setmetatable(h, Host)
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

  local frame_header = FrameHeader.from_raw_bytes(frame_version_byte, header_bytes)

  -- Receive frame body
  local body_bytes
  if frame_header.body_length > 0 then
    body_bytes, err = self.socket:receive(frame_header.body_length)
    if body_bytes == nil then
      return nil, err
    end
  end

  return FrameReader(frame_header, body_bytes)
end

function Host:send(request)
  request:set_version(self.protocol_version)

  local frame_reader, err = send_and_receive(self, request)
  if err then
    if err == "timeout" or err == "wantread" then -- cosocket/luasocket or LuaSec timeout
      return nil, Errors.TimeoutError(self.address)
    else
      return nil, Errors.SocketError(self.address, err)
    end
  end

  -- result, cql_error
  return frame_reader:parse()
end

local function startup(self)
  log.debug("Startup request. Trying to use protocol v"..self.protocol_version)

  local startup_req = Requests.StartupRequest()
  return self:send(startup_req)
end

local function change_keyspace(self, keyspace)
  log.debug("Keyspace request. Using keyspace: "..keyspace)

  local keyspace_req = Requests.KeyspaceRequest(keyspace)
  return self:send(keyspace_req)
end

local function send_auth(self, provider)
  local token = provider:initial_response()
  local auth_request = Requests.AuthResponse(token)
  local res, err = self:send(auth_request)
  if err then
    return nil, err
  elseif res and res.authenticated then
    return true
  end

  -- For other authenticators:
  --   evaluate challenge
  --   on authenticate success
end

function Host:connect()
  if self.connected then return true end

  new_socket(self)

  log.debug("Connecting to "..self.address)

  self:set_timeout(self.options.socket_options.connect_timeout)

  local ok, err = self.socket:connect(self.host, self.port)
  if ok ~= 1 then
    return false, Errors.SocketError(self.address, err), true
  end

  self:set_timeout(self.options.socket_options.read_timeout)

  if self.options.ssl_options.enabled then
    ok, err = self.socket:sslhandshake(false, nil, self.options.ssl_options.verify)
    if not ok then
      return false, Errors.SSLError(self.address, err)
    end
  end

  log.debug("Session connected to "..self.address)

  if self.socket:getreusedtimes() > 0 then
    -- No need for startup request
    log.debug("Socket retrieved from the connection pool, ",
              self.socket:getreusedtimes(), " times")
    self.connected = true
    return true
  end

  -- Startup request on first connection
  local ready = false
  local res, err = startup(self)
  if err then
    log.info("Startup request failed. "..err)
    -- Check for incorrect protocol version
    if err and err.code == CQL_Errors.PROTOCOL then
      if string_find(err.message, "Invalid or unsupported protocol version:", nil, true) then
        self:close()
        self:decrease_version()
        if self.protocol_version < MIN_PROTOCOL_VERSION then
          log.err("Connection could not find a supported protocol version.")
        else
          log.info("Decreasing protocol version to v"..self.protocol_version)
          return self:connect()
        end
      end
    end

    return false, err, true
  elseif res.must_authenticate then
    log.info("Host at "..self.address.." required authentication")
    if self.options.auth == nil then
      return nil, Errors.AuthenticationError("Host at "..self.address..
        " required authentication but no auth provider was configured for session")
    end

    local ok, err = send_auth(self, self.options.auth)
    if err then
      return false, Errors.AuthenticationError(err)
    elseif ok then
      ready = true
    end
  elseif res.ready then
    ready = true
  end

  if ready then
    log.debug("Host at "..self.address.." is ready with protocol v"..self.protocol_version)
    if self.options.keyspace ~= nil then
      local _, err = change_keyspace(self, self.options.keyspace)
      if err then
        log.err("Could not set keyspace. "..err)
        return false, err
      end
    end
    self.connected = true
    return true
  end
end

function Host:change_keyspace(keyspace)
  if self.connected then
    self.options.keyspace = keyspace

    local res, err = change_keyspace(self, keyspace)
    if err then
      log.err("Could not change keyspace for host "..self.address)
    end
    return res, err
  end
end

function Host:set_timeout(t)
  return self.socket:settimeout(t)
end

function Host:set_keep_alive()
  -- don't close if the connection was not opened yet
  if not self.connected then
    return true
  end

  -- tcpsock:setkeepalive() does not accept nil values, so this is a quick workaround
  -- see https://github.com/openresty/lua-nginx-module/pull/625
  local ok, err
  if self.options.socket_options.pool_timeout ~= nil then
    if self.options.socket_options.pool_size ~= nil then
      ok, err = self.socket:setkeepalive(self.options.socket_options.pool_timeout, self.options.socket_options.pool_size)
    else
      ok, err = self.socket:setkeepalive(self.options.socket_options.pool_timeout)
    end
  else
    ok, err = self.socket:setkeepalive()
  end

  if err then
    log.err("Could not set keepalive socket to "..self.address..". "..err)
    return ok, Errors.SocketError(self.address, err)
  end

  self.connected = false
  return true
end

function Host:close()
  -- don't try to close if the connection was not opened yet
  if not self.connected then
    return true
  end

  log.info("Closing connection to "..self.address..".")
  local _, err = self.socket:close()
  if err then
    log.err("Could not close socket to "..self.address..". "..err)
    return false, Errors.SocketError(self.address, err)
  end
  self.connected = false
  return true
end

function Host:set_down()
  local lock, lock_err, elapsed = lock_mutex(self.options.shm, "downing_"..self.address)
  if lock_err then
    return lock_err
  end

  if elapsed and elapsed == 0 then
    local host_infos, err = cache.get_host(self.options.shm, self.address)
    if err then
      return err
    end
    host_infos.unhealthy_at = time_utils.get_time()
    host_infos.reconnection_delay = self.reconnection_policy.next(self)
    log.warn("Setting host "..self.address.." as DOWN. Next retry in: "..host_infos.reconnection_delay.."ms.")
    self:close()
    local ok, err = cache.set_host(self.options.shm, self.address, host_infos)
    if not ok then
      return err
    end
  end

  lock_err = unlock_mutex(lock)
  if lock_err then
    return lock_err
  end
end

function Host:set_up()
  local host_infos, err = cache.get_host(self.options.shm, self.address)
  if err then
    return false, err
  end

  -- host was previously marked as DOWN (+ a safe delay)
  if host_infos.unhealthy_at ~= 0 and time_utils.get_time() - host_infos.unhealthy_at >= host_infos.reconnection_delay then
    log.warn("Setting host "..self.address.." as UP")
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

local RequestHandler = {}
RequestHandler.__index = RequestHandler

function RequestHandler:new(hosts, options)
  local o = {
    hosts = hosts,
    options = options,
    n_retries = 0
  }

  return setmetatable(o, RequestHandler)
end

function RequestHandler.get_first_coordinator(hosts)
  local errors = {}
  for _, host in ipairs(hosts) do
    local connected, err, maybe_down = host:connect()
    if not connected then
      if maybe_down then
        errors[host.address] = err
      else
        return nil, err
      end
    else
      return host
    end
  end

  return nil, Errors.NoHostAvailableError(errors)
end

function RequestHandler:get_next_coordinator()
  local errors = {}
  local iter = self.options.policies.load_balancing

  for i, host in iter(self.options.shm, self.hosts) do
    local can_host_be_considered_up, cache_err = host:can_be_considered_up()
    if cache_err then
      return nil, cache_err
    elseif can_host_be_considered_up then
      local connected, err, maybe_down = host:connect()
      if connected then
        self.coordinator = host
        return host
      elseif maybe_down then
        -- only on socket connect error
        -- might be a bad host, setting DOWN
        local cache_err = host:set_down()
        if cache_err then
          return nil, cache_err
        end
        errors[host.address] = err
      else
        return nil, err
      end
    else
      errors[host.address] = "Host considered DOWN"
    end
  end

  return nil, Errors.NoHostAvailableError(errors)
end

local function check_schema_consensus(request_handler)
  if #request_handler.hosts == 1 then
    return true
  end

  local local_query = Requests.QueryRequest("SELECT schema_version FROM system.local")
  local local_res, err = request_handler.coordinator:send(local_query)
  if local_res == nil or err then
    return nil, err
  end

  local peers_query = Requests.QueryRequest("SELECT schema_version FROM system.peers")
  local peers_res, err = request_handler.coordinator:send(peers_query)
  if peers_res == nil or err then
    return nil, err
  end

  local match = false
  if #peers_res > 0 and #local_res > 0 then
    match = true
    for _, peer_row in ipairs(peers_res) do
      if peer_row.schema_version ~= local_res[1].schema_version then
        match = false
        break
      end
    end
  end

  return match
end

function RequestHandler:wait_for_schema_consensus()
  log.info("Waiting for schema consensus")

  local match, t_diff, err
  local start = time_utils.get_time()

  repeat
    time_utils.wait(0.5)
    match, err = check_schema_consensus(self)
    t_diff = time_utils.get_time() - start
  until match or err ~= nil or t_diff >= self.options.protocol_options.max_schema_consensus_wait

  if err ~= nil then
    return err
  elseif not match then
    log.err("Waiting for schema consensus timed out. "..t_diff.." > "..self.options.protocol_options.max_schema_consensus_wait)
  end
end

function RequestHandler:send_on_next_coordinator(request)
  local coordinator, err = self:get_next_coordinator()
  if not coordinator or err then
    return nil, err
  end

  log.debug("Load balancing policy proposed to try host at: "..coordinator.address)

  return self:send(request)
end

function RequestHandler:send(request)
  if self.coordinator == nil then
    return self:send_on_next_coordinator(request)
  end

  local result, err = self.coordinator:send(request)
  if err then
    return self:handle_error(request, err)
  end

  -- Success! Make sure to re-up the node in case it was marked as DOWN
  local ok, cache_err = self.coordinator:set_up()
  if not ok then
    return nil, cache_err
  end

  if result.type == "SCHEMA_CHANGE" then
    local err = self:wait_for_schema_consensus()
    if err then
      log.warn("There was an error while waiting for the schema consensus between nodes: "..err)
    end
  end

  return result
end

function RequestHandler:handle_error(request, err)
  local retry_policy = self.options.policies.retry
  local decision = retry_policy.decisions.throw

  if err.type == "SocketError" then
    -- host seems unhealthy
    local cache_err = self.coordinator:set_down()
    if cache_err then
      return nil, cache_err
    end
    -- always retry, another node will be picked
    return self:retry(request)
  elseif err.type == "TimeoutError" then
    if self.options.query_options.retry_on_timeout then
      return self:retry(request)
    end
  elseif err.type == "ResponseError" then
    local request_infos = {
      handler = self,
      request = request,
      n_retries = self.n_retries
    }
    if err.code == CQL_Errors.OVERLOADED or err.code == CQL_Errors.IS_BOOTSTRAPPING or err.code == CQL_Errors.TRUNCATE_ERROR then
      -- always retry, we will hit another node
      return self:retry(request)
    elseif err.code == CQL_Errors.UNAVAILABLE_EXCEPTION then
      decision = retry_policy.on_unavailable(request_infos)
    elseif err.code == CQL_Errors.READ_TIMEOUT then
      decision = retry_policy.on_read_timeout(request_infos)
    elseif err.code == CQL_Errors.WRITE_TIMEOUT then
      decision = retry_policy.on_write_timeout(request_infos)
    elseif err.code == CQL_Errors.UNPREPARED then
      return self:prepare_and_retry(request)
    end
  end

  if decision == retry_policy.decisions.retry then
    return self:retry(request)
  end

  -- this error needs to be reported to the session
  return nil, err
end

function RequestHandler:retry(request)
  self.coordinator:close()
  self.n_retries = self.n_retries + 1
  log.info("Retrying request on next coordinator")
  return self:send_on_next_coordinator(request)
end

function RequestHandler:prepare_and_retry(request)
  local preparing_lock_key = string_format("0x%s_host_%s", request.query_id, self.coordinator.address)

  -- MUTEX
  local lock, lock_err, elapsed = lock_mutex(self.options.prepared_shm, preparing_lock_key)
  if lock_err then
    return nil, lock_err
  end

  if elapsed and elapsed == 0 then
    -- prepare on this host
    log.info("Query 0x"..request:hex_query_id().." not prepared on host "..self.coordinator.address..". Preparing and retrying.")
    local prepare_request = Requests.PrepareRequest(request.query)
    local res, err = self:send(prepare_request)
    if err then
      return nil, err
    end
    log.info("Prepared query for host "..self.coordinator.address)

    if request.query_id ~= res.query_id then
      log.warn(string_format("Unexpected difference between prepared query ids for query %s (%s ~= %s)", request.query, request.query_id, res.query_id))
      request.query_id = res.query_id
    end
  end

  lock_err = unlock_mutex(lock)
  if lock_err then
    return nil, "Error unlocking mutex for query preparation: "..lock_err
  end

  -- Send on the same coordinator as the one it was just prepared on
  return self:send(request)
end

--- Session
-- @section session

local Session = {}

function Session:new(options)
  local session_options, err = opts.parse_session(options)
  if err then
    return nil, err
  end

  local s = {
    options = session_options,
    hosts = {}
  }

  local host_addresses, cache_err = cache.get_hosts(session_options.shm)
  if cache_err then
    return nil, cache_err
  elseif host_addresses == nil then
    log.warn("No cluster infos in shared dict "..session_options.shm)
    if session_options.contact_points ~= nil then
      host_addresses, err = Cassandra.refresh_hosts(session_options)
      if host_addresses == nil then
        return nil, err
      end
    else
      return nil, Errors.DriverError("Options must contain contact_points to spawn session, or spawn a cluster in the init phase.")
    end
  end

  for _, addr in ipairs(host_addresses) do
    table_insert(s.hosts, Host:new(addr, session_options))
  end

  return setmetatable(s, {__index = self})
end

local function prepare_query(request_handler, query)
  -- If the query is found in the cache, all workers can access it.
  -- If it is not found, we might be in a cold-cache scenario. In that case,
  -- only one worker needs to
  local query_id, cache_err, prepared_key = cache.get_prepared_query_id(request_handler.options, query)
  if cache_err then
    return nil, cache_err
  elseif query_id == nil then
    -- MUTEX
    local prepared_key_lock = prepared_key.."_lock"
    local lock, lock_err, elapsed = lock_mutex(request_handler.options.prepared_shm, prepared_key_lock)
    if lock_err then
      return nil, "Could not create lock for prepare request: "..lock_err
    end

    if elapsed and elapsed == 0 then
      -- prepare query in the mutex
      log.info("Query not prepared in cluster yet. Preparing.")
      local prepare_request = Requests.PrepareRequest(query)
      local res, err = request_handler:send(prepare_request)
      if err then
        return nil, err
      elseif res.query_id == nil then
        return nil, "Could not retrieve query id from prepare request"
      end
      query_id = res.query_id
      local ok, cache_err = cache.set_prepared_query_id(request_handler.options, query, query_id)
      if not ok then
        return nil, "Could not insert query id in cache for prepared query: "..cache_err
      end
      log.info("Query prepared for host "..request_handler.coordinator.address)
    else
      -- once the lock is resolved, all other workers can retry to get the query, and should
      -- instantly succeed. We then skip the preparation part.
      query_id, cache_err = cache.get_prepared_query_id(request_handler.options, query)
      if cache_err then
        return nil, "Could not get query id from cache for prepared query: "..cache_err
      elseif query_id == nil then
        return nil, "No query id found in cache for prepared query"
      end
    end

    -- UNLOCK MUTEX
    lock_err = unlock_mutex(lock)
    if lock_err then
      return nil, "Error unlocking mutex for query for prepare request: "..lock_err
    end
  end

  return query_id
end

local function inner_execute(request_handler, query, args, query_options)
  if query_options.prepare then
    local query_id, err = prepare_query(request_handler, query)
    if err then
      return nil, err
    end

    -- Send on the same coordinator as the one it was just prepared on
    local prepared_request = Requests.ExecutePreparedRequest(query_id, query, args, query_options)
    return request_handler:send(prepared_request)
  end

  local query_request = Requests.QueryRequest(query, args, query_options)
  return request_handler:send_on_next_coordinator(query_request)
end

local function page_iterator(request_handler, query, args, query_options)
  local page = 0
  return function(query, previous_rows)
    if previous_rows and previous_rows.meta.has_more_pages == false then
      return nil -- End iteration after error
    end

    query_options.paging_state = previous_rows and previous_rows.meta.paging_state

    local rows, err = inner_execute(request_handler, query, args, query_options)

    -- If we have some results, increment the page
    if rows ~= nil and #rows > 0 then
      page = page + 1
    else
      if err then
        -- Just expose the error with 1 last iteration
        return {meta = {has_more_pages = false}}, err, page
      elseif rows.meta.has_more_pages == false then
        return nil -- End of the iteration
      end
    end

    return rows, err, page
  end, query, nil
end

--- Execute a query.
-- The session will choose a coordinator from the cached cluster topology according
-- to the configured load balancing policy. Once connected to it, it will perform the
-- given query. This operation is non-blocking in the context of ngx_lua because it
-- uses the cosocket API (coroutine based).
--
-- Queries can have parameters binded to it, they can be prepared, result sets can
-- be paginated, and more, depending on the given `query_options`.
--
-- If a node is not responding, it will be marked as being down. Since the state of
-- the cluster is shared accross all workers by the ngx.shared.DICT API, all the other
-- active sessions will be aware of it and will not attempt to connect to that node. The
-- configured reconnection policy will decide when it is time to try to connect to that
-- node again.
--
-- When selecting a large number of rows, it is a good practise to not increase the `page_size`
-- option, but to iterate over them. It is also useful to build web services. For such usages,
-- this function accepts a `paging_state` and an `auto_paging` options.
--
-- @usage
-- local res, err = session:execute [[
--   CREATE KEYSPACE IF NOT EXISTS my_keyspace
--   WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
-- ]]
--
-- local rows, err = session:execute("SELECT * FROM system.schema_keyspaces")
-- for i, row in ipairs(rows) do
--   print(row.keyspace_name)
-- end
--
-- local rows, err = session:execute("SELECT * FROM users WHERE age = ?", {42}, {prepare = true})
-- for i, row in ipairs(rows) do
--   print(row.username)
-- end
--
-- -- With auto-pagination
-- for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = 50, auto_paging = true}) do
--   -- rows contains the retrived rows for this page
--   -- err contains an eventual error and will interrupt the iteration if it is set
--   -- page contains the current page number, starting from 1
-- end
--
-- @param[type=string] query The CQL query to execute, possibly with placeholder for binded parameters.
-- @param[type=table] args *Optional* A list of parameters to be binded to the query's placeholders.
-- @param[type=table] query_options *Optional* Override the session`s `options.query_options` with the given values, for this request only.
-- @treturn table `result/rows`: A table describing the result. The content of this table depends on the type of the query. If an error occurred, this value will be `nil` and a second value describing the error is returned.
-- @treturn table `error`: A table describing the error that occurred.
function Session:execute(query, args, query_options)
  if self.terminated then
    return nil, Errors.NoHostAvailableError("Cannot reuse a session that has been shut down.")
  elseif type(query) ~= "string" then
    error("argument #1 must be a string", 2)
  end

  local options = table_utils.copy_args(self.options)
  options.query_options = table_utils.extend_table(options.query_options, query_options)

  local request_handler = RequestHandler:new(self.hosts, options)

  if options.query_options.auto_paging then
    return page_iterator(request_handler, query, args, options.query_options)
  end

  return inner_execute(request_handler, query, args, options.query_options)
end

--- Execute a batch of queries.
-- The session will chose a coordinator according to the load balancing policy and
-- execute the batch on it. By default, the session executes a `logged` batch. If given in
-- the `query_options`, the session can also execute `unlogged` and `counter` batches. The queries composing a batch can also be prepared.
--
-- Be weary that depending on the number of partitions involved in the given queries, using
-- batches can overload a coordinator, lead to performance drops and constitute an anti-pattern.
--
-- See the documentation on [CQL Batches](https://cassandra.apache.org/doc/cql3/CQL-2.2.html#batchStmt).
--
-- @usage
-- -- Basic logged batch
-- local res, err = session:batch({
--   {"INSERT INTO users(id, name) VALUES(123, 'Alice')"},
--   {"UPDATE users SET name = 'Alicia' WHERE id = 123"}
-- })
--
-- -- With binded parameters and prepared queries
-- local res, err = session:batch({
--   {"INSERT INTO users(id, name) VALUES(?, ?)", {cassandra.uuid(some_uuid), "Alice"}},
--   {"UPDATE users SET name = ? WHERE id = ?", {cassandra.uuid(some_uuid), "Alicia"}}
-- }, {prepare = true})
--
-- -- Unlogged batch
-- local res, err = session:batch({
--   {"INSERT INTO users(id, name) VALUES(123, 'Alice')"},
--   {"UPDATE users SET name = 'Alicia' WHERE id = 123"}
-- }, {logged = false})
--
-- -- Counter batch
-- local res, err = session:batch({
--   {"UPDATE table SET value = value + 1 WHERE key = 'counter'"},
--   {"UPDATE table SET value = value + 5 WHERE key = 'counter'"},
--   {"UPDATE table SET value = value + 2 WHERE key = ?", {"counter"}}
-- }, {counter = true})
--
-- @param[type=table] queries A list of CQL queries that constitutes the batch. Each value can contain either a string, or a table containing the query and its binded parameters.
-- @param[type=table] query_options *Optional* Override the session's `options.query_options` with the given values, for this batch only. Can also be used to specify other batch types.
-- @treturn table `result`: A table describing the result. Batch results are always `VOID` results. If an error occurred, this value will be `nil` and a second value describing the error is returned.
-- @treturn table `error`: A table describing the error that occurred.
function Session:batch(queries, query_options)
  local options = table_utils.copy_args(self.options)
  options.query_options = table_utils.extend_table({logged = true}, options.query_options, query_options)

  local request_handler = RequestHandler:new(self.hosts, options)

  if options.query_options.prepare then
    for i, q in ipairs(queries) do
      local query_id, err = prepare_query(request_handler, q[1])
      if err then
        return nil, err
      end
      queries[i].query_id = query_id
    end
  end

  local batch_request = Requests.BatchRequest(queries, options.query_options)
  -- with :send(), the same coordinator will be used if we prepared some queries,
  -- and a new one will be chosen if none were used yet.
  return request_handler:send(batch_request)
end

--- Switch the session's keyspace.
-- All underlying sockets of the session will switch to the given keyspace. Usually, the keyspace is set
-- by the `keyspace` option given to `spawn_session`, and it is a better practise to use another session
-- for another keyspace, but sometimes, it might be useful to switch a sesson to another keyspace.
--
-- @usage
-- local ok, err = session:set_keyspace("my_keyspace")
-- if not ok then
--   -- handle err
-- end
--
-- @param[type=string] keyspace The name of the keyspace to switch to.
-- @treturn boolean `ok`: True if successful, false otherwise. If false, a second value describing the error is returned.
-- @treturn table `error`: A table describing the error that occurred.
function Session:set_keyspace(keyspace)
  local errors = {}
  self.options.keyspace = keyspace
  for _, host in ipairs(self.hosts) do
    local _, err = host:change_keyspace(keyspace)
    if err then
      table_insert(errors, err)
    end
  end

  if #errors > 0 then
    return false, errors
  end

  return true
end

--- Put the underlying sockets into the connection pool and keep them alive.
-- When used inside of ngx_lua, the session will leverage the cosocket API, which maintains
-- a connection pool of sockets to re-use for better performance. Once the session has finished
-- its work, calling this function will allow other sessions to re-use the sockets, without having
-- to open new ones.
--
-- See the lua-nginx-module documentation on [tcpsocket:setkeepalive()](https://github.com/openresty/lua-nginx-module#tcpsocksetkeepalive).
--
-- The underlying `tcpsocket:setkeepalive()` function will be called with the session`s options values
-- defined in `socket_options.pool_timeout` and `socket_options.pool_size`. Those values are `nil` by default.
--
-- When called outside of ngx_lua, the sockets will be **closed**.
--
-- This function does not take any parameters and does not have any return values.
--
-- @usage
-- -- Execute queries...
-- session:set_keep_alive()
function Session:set_keep_alive()
  for _, host in ipairs(self.hosts) do
    host:set_keep_alive()
  end
end

--- Terminate a session.
-- Close all underlying sockets and free all data.
-- A session closed this way cannot be re-used.
--
-- This function does not take any parameters and does not have any return values.
function Session:shutdown()
  for _, host in ipairs(self.hosts) do
    host:close()
  end
  self.hosts = {}
  self.terminated = true
end

--- Cassandra
-- @section cassandra

--- Spawn a session to connect to the cluster.
-- Sessions are meant to be short-lived and many can be created in parallel. In the context
-- of ngx_lua, it makes perfect sense for a session to be spawned in a phase handler, and
-- quickly disposed of by putting the sockets it used back into the cosocket connection pool.
--
-- The session will retrieve the cluster topology from the configured shared dict or,
-- if not found, by connecting to one of the optionally given `contact_points`.
-- If you want to pre-load the cluster topology, see `spawn_cluster`.
-- The created session will use the configured load balancing policy to choose a
-- coordinator from the retrieved cluster topology on each query.
--
-- @usage
-- access_by_lua_block {
--     local cassandra = require "cassandra"
--     local session, err = cassandra.spawn_session {
--         shm = "cassandra",
--         contact_points = {"127.0.0.1", "127.0.0.2"}
--     }
--     if not session then
--         ngx.log(ngx.ERR, tostring(err))
--     end
--
--     -- execute query(ies)
--
--     session:set_keep_alive()
-- }
--
-- @param[type=table] options The session's options, including the shared dict name and **optionally* the contact points
-- @treturn table `session`: A instanciated session, ready to be used. If an error occurred, this value will be `nil` and a second value is returned describing the error.
-- @treturn table `error`: A table describing the error that occurred.
function Cassandra.spawn_session(options)
  return Session:new(options)
end

local SELECT_PEERS_QUERY = "SELECT peer,data_center,rack,rpc_address,release_version FROM system.peers"
local SELECT_LOCAL_QUERY = "SELECT release_version FROM system.local"

-- Retrieve cluster informations from a connected contact_point
function Cassandra.refresh_hosts(ops)
  local options = opts.parse_cluster(ops)
  local addresses = {}

  local lock, lock_err, elapsed = lock_mutex(options.shm, "refresh_hosts")
  if lock_err then
    return nil, lock_err
  end

  if elapsed and elapsed == 0 then
    log.info("Refreshing local and peers info")

    local contact_points_hosts = {}
    for _, contact_point in ipairs(options.contact_points) do
      local address = options.policies.address_resolution(contact_point)
      table_insert(contact_points_hosts, Host:new(address, options))
    end

    local coordinator, err = RequestHandler.get_first_coordinator(contact_points_hosts)
    if err then
      return nil, err
    end

    local peers_query = Requests.QueryRequest(SELECT_PEERS_QUERY)
    local local_query = Requests.QueryRequest(SELECT_LOCAL_QUERY)
    local hosts = {}

    local local_host = {
      unhealthy_at = 0,
      reconnection_delay = 0
    }
    hosts[coordinator.address] = local_host

    local rows, err = coordinator:send(local_query)
    if not rows then
      return nil, err
    end

    local release_version = rows[1].release_version
    local ok, cache_err = cache.set_cluster_info(options.shm, release_version)
    if not ok then
      return nil, cache_err
    end

    log.info("Local info retrieved")

    local rows, err = coordinator:send(peers_query)
    if err then
      return nil, err
    end

    for i, row in ipairs(rows) do
      if i > 1 and release_version ~= row.release_version then
        return nil, Errors.DriverError("nodes have different release versions ("..release_version.." vs "..row.release_version..")")
      end

      local address = options.policies.address_resolution(row["rpc_address"], options.protocol_options.default_port)
      log.info("Adding host "..address)
      hosts[address] = {
        unhealthy_at = 0,
        reconnection_delay = 0
      }
    end
    log.info("Peers info retrieved")
    log.info(string_format("Cluster infos retrieved in shared dict %s", options.shm))

    coordinator:close()

    -- Store cluster mapping for future sessions
    for addr, host in pairs(hosts) do
      table_insert(addresses, addr)
      local ok, cache_err = cache.set_host(options.shm, addr, host)
      if not ok then
        return nil, cache_err
      end
    end

    local ok, cache_err = cache.set_hosts(options.shm, addresses)
    if not ok then
      return nil, cache_err
    end
  else
    local cache_err
    addresses, cache_err = cache.get_hosts(options.shm)
    if cache_err then
      return nil, cache_err
    end
  end

  lock_err = unlock_mutex(lock)
  if lock_err then
    return nil, lock_err
  end

  return addresses
end

--- Load the cluster topology.
-- Iterate over the given `contact_points` and connects to the first one available
-- to load the cluster topology. All peers of the chosen contact point will be
-- retrieved and stored locally so that the load balancing policy can chose one
-- on each request that will be executed.
--
-- Use this function if you want to retrieve the cluster topology sooner than when
-- you will create your first `Session`.
--
-- @usage
-- init_worker_by_lua_block {
--     local cassandra = require "cassandra"
--     local cluster, err = cassandra.spawn_cluster {
--          shm = "cassandra",
--          contact_points = {"127.0.0.1"}
--     }
-- }
--
-- access_by_lua_block {
--     local cassandra = require "cassandra"
--     -- The cluster topology is already loaded at this point,
--     -- avoiding latency on your first request.
--     local session, err = cassandra.spawn_session {
--         shm = "cassandra"
--     }
-- }
--
-- @param[type=table] options The cluster's options, including the shared dict name and the contact points.
-- @treturn boolean `ok`: Success of the cluster topology retrieval. If false, a second value will be returned describing the error.
-- @treturn table `error`: An error in case the operation did not succeed.
function Cassandra.spawn_cluster(options)
  local cluster_options, err = opts.parse_cluster(options)
  if err then
    return false, err
  end

  local addresses, err = Cassandra.refresh_hosts(cluster_options)
  if addresses == nil then
    return false, err
  end

  return true
end

function Cassandra.get_cluster_info(options)
  return cache.get_cluster_info(options.shm)
end

--- Type serializer shorthands.
-- When binding parameters to a query from `execute`, some
-- types cannot be infered automatically and will require manual
-- serialization. Some other times, it can be useful to manually enforce
-- the type of a parameter.
--
-- See the documentation on [CQL Data Types](https://cassandra.apache.org/doc/cql3/CQL-2.2.html#types).
--
-- For this purpose, shorthands for type serialization are available
-- on the `Cassandra` table.
--
-- @usage
-- session:execute("SELECT * FROM users WHERE id = ?", {
--   cassandra.uuid("123e4567-e89b-12d3-a456-426655440000")
-- })
--
-- session:execute("INSERT INTO users(id, emails) VALUES(?, ?)", {
--   1,
--   cassandra.set({"john@foo.com", "john@bar.com"})
--})
--
-- @field unset Equivalent to the `null` CQL value. Useful to unset a field.
--     cassandra.unset()
-- @field uuid Serialize a 32 lowercase characters string to a CQL uuid.
--     cassandra.uuid("123e4567-e89b-12d3-a456-426655440000")
-- @field timestamp Serialize a 10 digits number into a CQL timestamp.
--     cassandra.timestamp(1405356926)
-- @field list
--     cassandra.list({"abc", "def"})
-- @field set
--     cassandra.set({"abc", "def"})
-- @field map
--     cassandra.map({foo = "bar"})
-- @field udt CQL UDT.
-- @field tuple CQL tuple.
-- @field inet CQL inet.
--     cassandra.inet("127.0.0.1")
--     cassandra.inet("2001:0db8:85a3:0042:1000:8a2e:0370:7334")
-- @field bigint CQL bigint.
--     cassandra.bigint(42000000000)
-- @field double CQL double.
--     cassandra.bigint(1.0000000000000004)
-- @field ascii CQL ascii.
-- @field blob CQL blob.
-- @field boolean CQL boolean.
--     cassandra.boolean(true)
-- @field counter CQL counter.
--     cassandra.counter(1)
-- @field decimal CQL decimal.
-- @field float CQL float.
--     cassandra.float(1.618033)
-- @field int CQL int.
--     cassandra.float(10)
-- @field text CQL text.
--     cassandra.float("hello world")
-- @field timeuuid CQL timeuuid.
-- @field varchar CQL varchar.
-- @field varint CQL varint.
-- @table type_serializers

local types_mt = {
  __index = function(self, key)
    if types.cql_types[key] ~= nil then
      return function(value)
        if value == nil then
          error("argument #1 required for '"..key.."' type shorthand", 2)
        end
        return {value = value, type_id = types.cql_types[key]}
      end
    elseif key == "unset" then
      return {value = "unset", type_id = "unset"}
    end

    return rawget(self, key)
  end
}

setmetatable(Cassandra, types_mt)

--- CQL data consistencies.
-- One of the `options` for `Session` and `execute` is the CQL consistency to use
-- with the queries executed. This table allowes you to specify other consistencies than the default `ONE`, by specifing the `query_options.consistency` option.
--
-- See the documentation on [CQL data consistency](http://docs.datastax.com/en/cassandra/2.1/cassandra/dml/dml_config_consistency_c.html).
--
-- @usage
-- -- For a session
-- cassandra.spawn_session {
--   query_options = {consistency = cassandra.consistencies.local}
-- }
--
-- -- For a single query
-- session:execute("SELECT * FROM users", nil, {consistency = cassandra.consistencies.local})
--
-- @field all CQL consistency `ALL`.
--     cassandra.consistencies.all
-- @field each_quorum CQL consistency `EACH_QUORUM`.
--     cassandra.consistencies.each_quorum
-- @field quorum CQL consistency `QUORUM`.
--     cassandra.consistencies.quorum
-- @field local_quorum CQL consistency `LOCAL_QUORUM`.
--     cassandra.consistencies.local_quorum
-- @field one CQL consistency `ONE`.
--     cassandra.consistencies.one
-- @field two CQL consistency `TWO`.
--     cassandra.consistencies.two
-- @field three CQL consistency `THREE`.
--     cassandra.consistencies.three
-- @field local_one CQL consistency `LOCAL_ONE`.
--     cassandra.consistencies.local_one
-- @field any CQL consistency `ANY`.
--     cassandra.consistencies.any
-- @field serial CQL consistency `SERIAL`.
--     cassandra.consistencies.seriam
-- @field local_serial CQL consistency `LOCAL_SERIAL`.
--     cassandra.consistencies.local_serial
-- @table consistencies

Cassandra.consistencies = types.consistencies

--- CQL Error types.
-- The library returns error that are tables. When the error type is `ResponseError`,
-- the table contains a field named `code`, which describes the CQL error. That code is
-- an hexadecimal value, that can be compared against the constants defined in this table.
-- @field SERVER something unexpected happened. This indicates a server-side bug.
-- @field PROTOCOL some client message triggered a protocol violation (for instance a `QUERY` message is sent before a `STARTUP`)
-- @field BAD_CREDENTIALS `CREDENTIALS` request failed because Cassandra did not accept the provided credentials.
-- @field UNAVAILABLE_EXCEPTION the query could not be processed with respect to the given concurrency.
--
-- (This error could return more informations but it is **not yet supported**.)
--
-- @field OVERLOADED the request cannot be processed because the coordinator node is overloaded.
-- @field IS_BOOTSTRAPPING the request was a read request but the coordinator node is bootstrapping.
-- @field TRUNCATE_ERROR error during a truncation error.
-- @field WRITE_TIMEOUT timeout exception during a write request. (This error could return more informations but it is **not yet supported**.)
-- @field READ_TIMEOUT timeout exception during a read request. (This error could return more informations but it is **not yet supported**.)
-- @field SYNTAX_ERROR the submitted query has a syntax error.
-- @field UNAUTHORIZED the logged user doesn't have the right to perform the query.
-- @field INVALID the query is syntactically correct but invalid.
-- @field CONFIG_ERROR the query is invalid because of some configuration issue.
-- @field ALREADY_EXISTS the query attempted to create a keyspace or a table that was already existing. (This error could return more informations but it is **not yet supported**.)
-- @field UNPREPARED Can be thrown while a prepared statement tries to be executed if the provide prepared statement ID is not known by this host. (This error could return more informations but it is **not yet supported**.)
-- @table cql_errors

Cassandra.cql_errors = types.ERRORS

local DEFAULT_AUTH_PROVIDERS = {
  PlainTextProvider = require "cassandra.auth.plain_text_provider"
}

for k, v in pairs(DEFAULT_AUTH_PROVIDERS ) do
  DEFAULT_AUTH_PROVIDERS[k] = setmetatable({}, {
    __call = function(self, ...)
      v.__index = v
      local provider = setmetatable({}, v)
      provider:new(...)
      return provider
    end
  })
end

---
-- Authentication providers that can be instanciated and given to a `session`'s options.
-- @field PlainTextProvider provider for plain text password authentication challenge (like `PasswordAuthenticator`).
--     cassandra.auth.PlainTextProvider("username", "password")
-- @table auth
Cassandra.auth = DEFAULT_AUTH_PROVIDERS

return Cassandra
