local FrameHeader = require "cassandra.types.frame_header"
local FrameReader = require "cassandra.frame_reader"
local Requests = require "cassandra.requests"
local socket = require "cassandra.socket"
local types = require "cassandra.types"

local setmetatable = setmetatable
local cql_errors = types.errors
local pairs = pairs
local find = string.find

local MIN_PROTOCOL_VERSION = 2
local DEFAULT_PROTOCOL_VERSION = 3

local _Host = {
  cql_errors = cql_errors
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
    protocol_version = opts.protocol_version or DEFAULT_PROTOCOL_VERSION,
    ssl = opts.ssl,
    verify = opts.verify,
    cert = opts.cert,
    cafile = opts.cafile,
    key = opts.key,
    -- auth = opts.auth
  }

  return setmetatable(host, _Host)
end

function _Host:send(request)
  if not self.sock then
    return nil, "no socket created"
  end

  request:set_version(self.protocol_version)

  local frame = request:get_full_frame()
  local sent, err = self.sock:send(frame)
  if not sent then return nil, err end

  -- receive frame version byte
  local v_byte, err = self.sock:receive(1)
  if not v_byte then return nil, err end

  -- -1 because of the v_byte we just read
  local n_bytes = FrameHeader.size_from_byte(v_byte) - 1

  -- receive frame header
  local header_bytes, err = self.sock:receive(n_bytes)
  if not header_bytes then return nil, err end

  local frame_header = FrameHeader.from_raw_bytes(v_byte, header_bytes)

  -- receive frame body
  local body_bytes
  if frame_header.body_length > 0 then
    body_bytes, err = self.sock:receive(frame_header.body_length)
    if not body_bytes then return nil, err end
  end

  local frame_reader = FrameReader(frame_header, body_bytes)

  -- res, err, cql_err_code
  return frame_reader:parse()
end

local function send_startup(self)
  local startup_req = Requests.StartupRequest()
  return self:send(startup_req)
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
  if not ok then return nil, err end

  if self.ssl then
    ok, err = ssl_handshake(self)
    if not ok then return nil, err end
  end

  local reused, err = self.sock:getreusedtimes()
  if not reused then return nil, err end

  if self.sock:getreusedtimes() < 1 then
    -- startup request on first connection
    local res, err, code = send_startup(self)
    if not res then
      if code == cql_errors.PROTOCOL and
         find(err, "Invalid or unsupported protocol version", nil, true) then
        -- too high protocol version
        self.sock:close()
        self.protocol_version = self.protocol_version - 1
        if self.protocol_version < MIN_PROTOCOL_VERSION then
          return nil, "could not find a supported protocol version"
        end
        return self:connect()
      end

      -- real connection issue, host could be down?
      return nil, err, true
    elseif res.must_authenticate then
      -- TODO: auth
    end

    if self.keyspace then
      -- TODO: since this not sent when the connection was retrieved
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
  local keyspace_req = Requests.KeyspaceRequest(keyspace)
  return self:send(keyspace_req)
end

function _Host:prepare(query)
  local prepare_request = Requests.PrepareRequest(query)
  return self:send(prepare_request)
end

local query_options = {
  consistency = types.consistencies.one,
  serial_consistency = types.consistencies.serial,
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

local function execute(self, query, args, opts)
  local request = opts.prepared and
    -- query is the prepared queryid
    Requests.ExecutePreparedRequest(query, args, opts)
    or
    Requests.QueryRequest(query, args, opts)

  return self:send(request)
end

local function page_iterator(self, query, args, opts)
  local page = 0
  return function(_, p_rows)
    local meta = p_rows.meta
    if not meta.has_more_pages then return end -- end after error

    opts.paging_state = meta.paging_state

    local rows, err = execute(self, query, args, opts)
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

function _Host:execute(query, args, options)
  local opts = get_opts(options)
  return execute(self, query, args, opts)
end

function _Host:iterate(query, args, options)
  return page_iterator(self, query, args, get_opts(options))
end

function _Host:batch(queries, options)
  local opts = get_opts(options)
  local batch_request = Requests.BatchRequest(queries, opts)
  return self:send(batch_request)
end

function _Host:__tostring()
  return "<Cassandra socket: "..tostring(self.sock)..">"
end

return _Host
