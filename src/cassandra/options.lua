local types = require "cassandra.types"
local remove = table.remove
local pairs = pairs
local type = type

local function extend_table(...)
  local sources = {...}
  local values = remove(sources)

  for _, source in ipairs(sources) do
    for k in pairs(source) do
      if values[k] == nil then
        values[k] = source[k]
      end
      if type(source[k]) == "table" and type(values[k]) == "table" then
        extend_table(source[k], values[k])
      end
    end
  end

  return values
end

--- Defaults
-- @section defaults

-- Nil values are stubs for the sole purpose of documenting their availability.
local DEFAULTS = {
  -- shm = nil,
  -- prepared_shm = nil,
  -- contact_points = {},
  -- keyspace = nil,
  policies = {
    address_resolution = require "cassandra.policies.address_resolution",
    load_balancing = require("cassandra.policies.load_balancing").SharedRoundRobin,
    reconnection = require("cassandra.policies.reconnection").SharedExponential(1000, 10 * 60 * 1000),
    retry = require("cassandra.policies.retry"),
  },
  query_options = {
    consistency = types.consistencies.one,
    serial_consistency = types.consistencies.serial,
    page_size = 1000,
    paging_state = nil,
    auto_paging = false,
    prepare = false,
    retry_on_timeout = true
  },
  protocol_options = {
    default_port = 9042,
    max_schema_consensus_wait = 10000
  },
  socket_options = {
    connect_timeout = 1000, -- ms
    read_timeout = 2000 -- ms
    -- pool_timeout = nil,
    -- pool_size = nil
  },
  ssl_options = {
    enabled = false
  -- key = nil,
  -- certificate = nil,
  -- ca = nil, -- stub
  -- verify = false
  },
  -- auth = nil
}

local opts_mt = {}
opts_mt.__index = opts_mt

function opts_mt:extend_query_options(...)
  return extend_table(self.query_options, ...)
end

local function parse_session(options, lvl)
  if options == nil then options = {} end
  extend_table(DEFAULTS, options)

  -- keyspace

  if options.keyspace ~= nil and type(options.keyspace) ~= "string" or options.keyspace == "" then
    return nil, "keyspace must be a valid string"
  end

  -- shms

  if options.shm == nil then
    return nil, "shm is required for spawning a cluster/session"
  end
  if type(options.shm) ~= "string" then
    return nil, "shm must be a string"
  end
  if options.shm == "" then
    return nil, "shm must be a valid string"
  end
  if options.prepared_shm == nil then
    options.prepared_shm = options.shm
  end
  if type(options.prepared_shm) ~= "string" then
    return nil, "prepared_shm must be a string"
  end
  if options.prepared_shm == "" then
    return nil, "prepared_shm must be a valid string"
  end

  -- protocol options

  if type(options.protocol_options.default_port) ~= "number" then
    return nil, "protocol default_port must be a number"
  end
  if type(options.protocol_options.max_schema_consensus_wait) ~= "number" then
    return nil, "protocol max_schema_consensus_wait must be a number"
  end

  -- policies

  if type(options.policies.address_resolution) ~= "function" then
    return nil, "address_resolution policy must be a function"
  end

  -- query options

  if type(options.query_options.page_size) ~= "number" then
    return nil, "query page_size must be a number"
  end

  -- socket options

  if type(options.socket_options) ~= "table" then
    return nil, "socket_options must be a table"
  end
  if type(options.socket_options.connect_timeout) ~= "number" then
    return nil, "socket connect_timeout must be a number"
  end
  if type(options.socket_options.read_timeout) ~= "number" then
    return nil, "socket read_timeout must be a number"
  end
  if options.socket_options.pool_timeout ~= nil and type(options.socket_options.pool_timeout) ~= "number" then
    return nil, "socket pool_timeout must be a number"
  end
  if options.socket_options.pool_size ~= nil and type(options.socket_options.pool_size) ~= "number" then
    return nil, "socket pool_size must be a number"
  end

  -- ssl options

  if type(options.ssl_options) ~= "table" then
    return nil, "ssl_options must be a table"
  end
  if type(options.ssl_options.enabled) ~= "boolean" then
    return nil, "ssl_options.enabled must be a boolean"
  end

  -- auth provider

  if options.auth then
    if type(options.auth) ~= "table" then
      return nil, "auth provider must be a table"
    elseif type(options.auth.initial_response) ~= "function" then
      return nil, "auth provider must implement initial_response()"
    end
  end

  return setmetatable(options, opts_mt)
end

local function parse_cluster(options)
  local err

  options, err = parse_session(options)
  if err then
    return nil, err
  end

  if type(options.contact_points) ~= "table" then
    return nil, "contact_points must be a table"
  end
  if #options.contact_points < 1 then
    return nil, "contact_points must contain at least one contact point"
  end

  options.keyspace = nil -- it makes no sense to use keyspace in this context

  return options
end

return {
  parse_cluster = parse_cluster,
  parse_session = parse_session
}
