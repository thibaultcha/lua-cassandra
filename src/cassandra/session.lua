local utils = require "cassandra.utils"

local _M = {
  CQL_VERSION = "3.0.0"
}

_M.__index = _M

local function send_frame_and_get_response(self, op_code, frame_body, tracing)
  local bytes, response, err
  local frame = self.writer:build_frame(op_code, frame_body, tracing)
  bytes, err = self.socket:send(frame)
  if not bytes then
    return nil, string.format("Failed to send frame to %s: %s", self.host, err)
  end
  response, err = self.reader:receive_frame(self)
  if not response then
    return nil, err
  end
  return response
end

local function startup(self)
  local frame_body = self.marshaller.string_map_representation({CQL_VERSION = _M.CQL_VERSION})
  local response, err = send_frame_and_get_response(self, self.constants.op_codes.STARTUP, frame_body)
  if not response then
    return false, err
  end
  if response.op_code ~= self.constants.op_codes.READY then
    return false, "server is not ready"
  end
  return true
end

-- Connect a session to a node coordinator.
-- @throw Any error due to a wrong usage of the driver.
-- @return `connected` A boolean indicating the success of the connection.
-- @return `err`       Any server/client error encountered during the connection.
function _M:connect(contact_points, port)
  if port == nil then port = 9042 end
  if contact_points == nil then
    error("no contact points provided", 2)
  elseif type(contact_points) == "table" then
    -- shuffle the contact points so we don't try  to connect always on the same order,
    -- avoiding pressure on the same node cordinator.
    utils.shuffle_array(contact_points)
  else
    contact_points = {contact_points}
  end

  if not self.socket then
    error("session does not have a socket, create a new session first", 2)
  end

  local ok, err
  for _, contact_point in ipairs(contact_points) do
    -- Extract port if string is of the form "host:port"
    local host, host_port = utils.split_by_colon(contact_point)
    if not host_port then -- Default port is the one given as parameter
      host_port = port
    end
    ok, err = self.socket:connect(host, host_port)
    if ok then
      self.host = host
      self.port = host_port
      break
    end
  end

  if not ok then
    return false, err
  end

  if not self.ready then
    self.ready, err = startup(self)
    if not self.ready then
      return false, err
    end
  end

  return true
end

-- Close a session.
-- Wrapper around the cosocket (or luasocket) `:close()`.
-- @throw Any error due to a wrong usage of the driver.
-- @see http://wiki.nginx.org/HttpLuaModule#tcpsock:close
-- @see http://w3.impa.br/~diego/software/luasocketp.html#close
function _M:close()
  if not self.socket then
    error("session does not have a socket, create a new session first", 2)
  end
  return self.socket:close()
end

-- Default query options.
-- @see `:execute()`
local default_options = {
  page_size = 5000,
  auto_paging = false,
  tracing = false
}

local function page_iterator(session, operation, args, options)
  local page = 0
  local rows, err
  return function(paginated_operation, previous_rows)
    if previous_rows and previous_rows.meta.has_more_pages == false then
      return nil -- End iteration after error
    end

    rows, err = session:execute(paginated_operation, args, {
      page_size = options.page_size,
      paging_state =  previous_rows and previous_rows.meta.paging_state
    })

    -- If we have some results, increment the page
    if rows ~= nil and #rows > 0 then
      page = page + 1
    else
      if err then
        -- Just expose the error with 1 last iteration
        return {meta={has_more_pages=false}}, err, page
      elseif rows.meta.has_more_pages == false then
        return nil -- End of the iteration
      end
    end

    return rows, err, page
  end, operation, nil
end

-- Execute an operation (string query, prepared statement, batch statement).
-- Will send the query, parse the response and return it.
-- @param  `operation` The operation to execute.
-- @param  `args`      (Optional) An array of arguments to bind to the operation.
-- @param  `options`   (Optional) A table of options to assign to this query.
-- @return `response`  The parsed response from Cassandra.
-- @return `err`       Any error encountered during the execution.
function _M:execute(operation, args, options)
  if not options then options = {} end
  -- Default options
  if not options.consistency_level then
    options.consistency_level = self.constants.consistency.ONE
  end
  for k in pairs(default_options) do
    if options[k] == nil then options[k] = default_options[k] end
  end

  if options.auto_paging then
    return page_iterator(self, operation, args, options)
  end

  local frame_body, op_code = self.writer:build_body(operation, args, options)
  local response, err = send_frame_and_get_response(self, op_code, frame_body, options.tracing)
  if not response then
    return nil, err
  end

  return self.reader:parse_response(response)
end

function _M:set_keyspace(keyspace)
  return self:execute(string.format("USE \"%s\"", keyspace))
end

function _M:prepare(query, tracing)
  local frame_body = self.marshaller.long_string_representation(query)
  local response, err = send_frame_and_get_response(self, self.constants.op_codes.PREPARE, frame_body, tracing)
  if not response then
    return nil, err
  end

  return self.reader:parse_response(response)
end

return _M
