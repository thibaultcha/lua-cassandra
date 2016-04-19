local socket = require "cassandra.socket"
local cql = require "cassandra.cql"

local setmetatable = setmetatable
local requests = cql.requests
local pairs = pairs
local find = string.find

local _Host = {
  cql_errors = cql.errors,
  consistencies = cql.consistencies,
  auth_providers = require "cassandra.auth"
}

_Host.__index = _Host

function _Host.new(opts)
  opts = opts or {}
  local sock, err = socket.tcp()
  if err then return nil, err end

  local host = {
    sock = sock,
    host = opts.host or "127.0.0.1",
    port = opts.port or 9042,
    keyspace = opts.keyspace,
    protocol_version = opts.protocol_version or cql.def_protocol_version,
    ssl = opts.ssl,
    verify = opts.verify,
    cert = opts.cert,
    cafile = opts.cafile,
    key = opts.key,
    auth = opts.auth
  }

  return setmetatable(host, _Host)
end

function _Host:send(request)
  if not self.sock then
    return nil, "no socket created"
  end

  local frame = request:build_frame(self.protocol_version)
  local sent, err = self.sock:send(frame)
  if not sent then return nil, err end

  -- receive frame version byte
  local v_byte, err = self.sock:receive(1)
  if not v_byte then return nil, err end

  -- -1 because of the v_byte we just read
  local version, n_bytes = cql.frame_reader.version(v_byte)

  -- receive frame header
  local header_bytes, err = self.sock:receive(n_bytes)
  if not header_bytes then return nil, err end

  local header = cql.frame_reader.read_header(version, header_bytes)

  -- receive frame body
  local body_bytes
  if header.body_length > 0 then
    body_bytes, err = self.sock:receive(header.body_length)
    if not body_bytes then return nil, err end
  end

  -- res, err, cql_err_code
  return cql.frame_reader.read_body(header, body_bytes)
end

local function send_startup(self)
  local startup_req = requests.startup.new()
  return self:send(startup_req)
end

local function send_auth(self)
  local token = self.auth:initial_response()
  local auth_request = requests.auth_response.new(token)
  local res, err = self:send(auth_request)
  if not res then
    return nil, err
  elseif res and res.authenticated then
    return true
  end
end

local function ssl_handshake(self)
  local params = {
    key = self.key,
    cafile = self.cafile,
    cert = self.cert
  }

  return self.sock:sslhandshake(false, nil, self.verify, params)
end

function _Host:connect()
  if not self.sock then
    return nil, "no socket created"
  end

  local ok, err = self.sock:connect(self.host, self.port)
  if not ok then return nil, err, true end

  if self.ssl then
    ok, err = ssl_handshake(self)
    if not ok then return nil, err end
  end

  local reused, err = self.sock:getreusedtimes()
  if not reused then return nil, err end

  if reused < 1 then
    -- startup request on first connection
    local res, err, code = send_startup(self)
    if not res then
      if code == cql.errors.PROTOCOL and
         find(err, "Invalid or unsupported protocol version", nil, true) then
        -- too high protocol version
        self.sock:close()
        self.protocol_version = self.protocol_version - 1
        if self.protocol_version < cql.min_protocol_version then
          return nil, "could not find a supported protocol version"
        end
        return self:connect()
      end

      -- real connection issue, host could be down?
      return nil, err, true
    elseif res.must_authenticate then
      if not self.auth then
        return nil, "authentication required"
      end

      local ok, err = send_auth(self)
      if not ok then return nil, err end
    end

    if self.keyspace then
      -- TODO: since this not sent when the socket was retrieved
      -- from the connection pool, we must document that calling
      -- set_keyspace() manually is required if they interact with
      -- several at once.
      local res, err = self:set_keyspace(self.keyspace)
      if not res then return nil, err end
    end
  end

  return true
end

function _Host:settimeout(...)
  if not self.sock then
    return nil, "no socket created"
  end
  return self.sock:settimeout(...)
end

function _Host:setkeepalive(...)
  if not self.sock then
    return nil, "no socket created"
  end
  return self.sock:setkeepalive(...)
end

function _Host:close(...)
  if not self.sock then
    return nil, "no socket created"
  end
  return self.sock:close(...)
end

function _Host:set_keyspace(keyspace)
  local keyspace_req = requests.keyspace.new(keyspace)
  return self:send(keyspace_req)
end

function _Host:prepare(query)
  local prepare_request = requests.prepare.new(query)
  return self:send(prepare_request)
end

local query_options = {
  consistency = cql.consistencies.one,
  serial_consistency = cql.consistencies.serial,
  page_size = 1000,
  paging_state = nil,
  auto_paging = false,
  -- execute with a prepared query id
  prepared = false,
  -- batch
  logged = true,
  counter = false,
  timestamp = nil
}

local function get_opts(o)
  if not o then
    return query_options
  else
    local opts = {
      paging_state = o.paging_state,
      timestamp = o.timestamp
    }
    for k,v in pairs(query_options) do
      if o[k] == nil then
        opts[k] = v
      else
        opts[k] = o[k]
      end
    end
    return opts
  end
end

_Host.get_request_opts = get_opts

local function page_iterator(self, query, args, opts)
  local page = 0
  return function(_, p_rows)
    local meta = p_rows.meta
    if not meta.has_more_pages then return end -- end after error

    opts.paging_state = meta.paging_state

    local rows, err = self:execute(query, args, opts)
    if rows and #rows > 0 then
      page = page + 1
    elseif err then -- expose the error with one more iteration
      rows = {meta = {has_more_pages = false}}
    else -- end of iteration
      return nil
    end

    return rows, err, page
  end, nil, {meta = {has_more_pages = true}}
  -- nil: our iteration has no invariant state, our control variable is
  -- the rows themselves
end

_Host.page_iterator = page_iterator

function _Host:execute(query, args, options)
  local opts = get_opts(options)
  local request = opts.prepared and
    -- query is the prepared queryid
    requests.execute_prepared.new(query, args, opts)
    or
    requests.query.new(query, args, opts)

  return self:send(request)
end

function _Host:iterate(query, args, options)
  return page_iterator(self, query, args, get_opts(options))
end

function _Host:batch(queries, options)
  local batch_request = requests.batch.new(queries, get_opts(options))
  return self:send(batch_request)
end

function _Host:__tostring()
  return "<Cassandra socket: "..tostring(self.sock)..">"
end

------------------
-- CQL serializers
------------------

for cql_t_name, cql_t in pairs(cql.types) do
  _Host[cql_t_name] = function(val)
    if val == nil then
      error("bad argument #1 to '"..cql_t_name.."' (got nil)", 2)
    end
    return {val = val, __cql_type = cql_t}
  end
end

_Host.unset = cql.t_unset

return _Host
