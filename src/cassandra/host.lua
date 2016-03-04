local FrameHeader = require "cassandra.types.frame_header"
local FrameReader = require "cassandra.frame_reader"
local Requests = require "cassandra.requests"
local socket = require "cassandra.socket"
local types = require "cassandra.types"

local setmetatable = setmetatable
local cql_errors = types.errors
local find = string.find

local MIN_PROTOCOL_VERSION = 2
local DEFAULT_PROTOCOL_VERSION = 3

local _Host = {
  cql_errors = cql_errors
}

_Host.__index = _Host

function _Host.new(opts)
  local sock, err = socket.tcp()
  if err then return nil, err end

  if not opts then opts = {} end

  local host = {
    sock = sock,
    host = opts.host or "127.0.0.1",
    port = opts.port or 9042,
    protocol_version = opts.protocol_version or DEFAULT_PROTOCOL_VERSION,
    -- ssl = opts.ssl
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

function _Host:connect()
  if not self.sock then
    return nil, "no socket created"
  end

  local ok, err = self.sock:connect(self.host, self.port)
  if not ok then return nil, err end

  -- TODO: SSL handshake

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

function _Host:execute(query, args, opts, prepared)
  local request
  if opts and opts.prepared then
    -- query is the prepared queryid
    request = Requests.ExecutePreparedRequest(query, args, opts)
  else
    request = Requests.QueryRequest(query, args, opts)
  end

  return self:send(request)
end

function _Host:prepare(query)
  local prepare_request = Requests.PrepareRequest(query)
  return self:send(prepare_request)
end

function _Host:batch(queries, opts)
  local batch_request = Requests.BatchRequest(queries, opts)
  return self:send(batch_request)
end

function _Host:__tostring()
  return "<Cassandra socket: "..tostring(self.sock)..">"
end

return _Host
