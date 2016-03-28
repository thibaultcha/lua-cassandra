local types = require "cassandra.types"
local setmetatable = setmetatable
local remove = table.remove
local ipairs = ipairs
local pairs = pairs
local type = type

--- Defaults
-- @section defaults

-- Nil values are stubs for the sole purpose of documenting their availability.
local DEFAULTS = {
  -- shm = nil,
  -- prepared_shm = nil,
  -- contact_points = {},
  -- keyspace = nil,
  policies = {
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
    max_schema_consensus_wait = 10000 -- ms
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

local opts_mt = {}
opts_mt.__index = opts_mt

function opts_mt:extend_query_options(...)
  return extend_table(self.query_options, ...)
end

--- Parsing
-- @section parsing

local _M = {}

function _M.parse(opts)
  local options = extend_table(DEFAULTS, opts)

  if not options.shm then
    return nil, "shm is required"
  elseif not options.contact_points then
    return nil, "contact_points is required"
  elseif options.prepared_shm == nil then
    options.prepared_shm = options.shm
  end

  return setmetatable(options, opts_mt)
end

return _M
