local utils = require "cassandra.utils.table"
local errors = require "cassandra.errors"

--- CONST
-- @section constants

local DEFAULTS = {
  contact_points = {},
  keyspace = "",
  print_log_level = "ERR",
  policies = {
    address_resolution = require "cassandra.policies.address_resolution"
  },
  protocol_options = {
    default_port = 9042
  }
}

local function parse(options)
  if options == nil then options = {} end

  utils.extend_table(DEFAULTS, options)

  if type(options.contact_points) ~= "table" then
    error("contact_points must be a table", 3)
  end

  if not utils.is_array(options.contact_points) then
    error("contact_points must be an array (integer-indexed table)")
  end

  if #options.contact_points < 1 then
    error("contact_points must contain at least one contact point")
  end

  if type(options.keyspace) ~= "string" then
    error("keyspace must be a string")
  end

  assert(type(options.protocol_options.default_port) == "number", "protocol default_port must be a number")
  assert(type(options.policies.address_resolution) == "function", "address_resolution policy must be a function")

  return options
end

return {
  parse = parse
}
