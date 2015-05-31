local utils = require "cassandra.utils"

local _M = {}

local error_mt = {}
error_mt = {
  __tostring = function(self)
    return self.message
  end,
  __concat = function (a, b)
    if getmetatable(a) == error_mt then
      return a.message..b
    else
      return a..b.message
    end
  end
}

local function read_error(session, buffer)
  local error_code = session.unmarshaller.read_int(buffer)
  local error_code_translation = session.constants.error_codes_translation[error_code]
  local error_message = session.unmarshaller.read_string(buffer)
  local err = {
    code = error_code,
    message = string.format("Cassandra returned error (%s): %s", error_code_translation, error_message),
    raw_message = error_message
  }
  return setmetatable(err, error_mt)
end

-- Make a session listen for a response and decode the received frame
-- @param `session`       The session on which to listen for a response.
-- @return `parsed_frame` The parsed frame ready to be read.
-- @return `err`          Any error encountered during the receiving.
function _M.read_frame(session)
  local unmarshaller = session.unmarshaller

  local header, err = session.socket:receive(8)
  if not header then
    return nil, string.format("Failed to read frame header from %s: %s", self.host, err)
  end
  local header_buffer = unmarshaller.create_buffer(header)
  local version = unmarshaller.read_raw_byte(header_buffer)
  local flags = unmarshaller.read_raw_byte(header_buffer)
  local stream = unmarshaller.read_raw_byte(header_buffer)
  local op_code = unmarshaller.read_raw_byte(header_buffer)
  local length = unmarshaller.read_int(header_buffer)

  local body, tracing_id
  if length > 0 then
    body, err = session.socket:receive(length)
    if not body then
      return nil, string.format("Failed to read frame body from %s: %s", self.host, err)
    end
  else
    body = ""
  end

  if version ~= session.constants.version_codes.RESPONSE then
    return nil, string.format("Invalid response version received from %s", self.host)
  end

  local body_buffer = unmarshaller.create_buffer(body)
  if flags == 0x02 then -- tracing
    tracing_id = unmarshaller.read_uuid(string.sub(body, 1, 16))
    body_buffer.pos = 17
  end

  if op_code == session.constants.op_codes.ERROR then
    return nil, read_error(session, body_buffer)
  end

  return {
    flags = flags,
    stream = stream,
    op_code = op_code,
    buffer = body_buffer,
    tracing_id = tracing_id
  }
end

local function parse_metadata(session, buffer)
  -- Flags parsing
  local flags = session.unmarshaller.read_int(buffer)
  local global_tables_spec = utils.hasbit(flags, session.constants.rows_flags.GLOBAL_TABLES_SPEC)
  local has_more_pages = utils.hasbit(flags, session.constants.rows_flags.HAS_MORE_PAGES)
  local columns_count = session.unmarshaller.read_int(buffer)

  -- Potential paging metadata
  local paging_state
  if has_more_pages then
    paging_state = session.unmarshaller.read_bytes(buffer)
  end

  -- Potential global_tables_spec metadata
  local global_keyspace_name, global_table_name
  if global_tables_spec then
    global_keyspace_name = session.unmarshaller.read_string(buffer)
    global_table_name = session.unmarshaller.read_string(buffer)
  end

  -- Columns metadata
  local columns = {}
  for _ = 1, columns_count do
    local ksname = global_keyspace_name
    local tablename = global_table_name
    if not global_tables_spec then
      ksname = session.unmarshaller.read_string(buffer)
      tablename = session.unmarshaller.read_string(buffer)
    end
    local column_name = session.unmarshaller.read_string(buffer)
    columns[#columns + 1] = {
      name = column_name,
      type = session.unmarshaller.read_option(buffer),
      table = tablename,
      keyspace = ksname
    }
  end

  return {
    columns = columns,
    paging_state = paging_state,
    columns_count = columns_count,
    has_more_pages = has_more_pages
  }
end

local function parse_rows(session, buffer, metadata)
  local columns = metadata.columns
  local columns_count = metadata.columns_count
  local rows_count = session.unmarshaller.read_int(buffer)
  local values = {}
  local row_mt = {
    __index = function(t, i)
      -- allows field access by position/index, not column name only
      local column = columns[i]
      if column then
        return t[column.name]
      end
      return nil
    end,
    __len = function() return columns_count end
  }
  for _ = 1, rows_count do
    local row = setmetatable({}, row_mt)
    for i = 1, columns_count do
      local value = session.unmarshaller.read_value(buffer, columns[i].type)
      row[columns[i].name] = value
    end
    values[#values + 1] = row
  end
  assert(buffer.pos == #(buffer.str) + 1)
  return values
end

function _M.parse_response(session, response)
  local result
  local result_kind = session.unmarshaller.read_int(response.buffer)

  if result_kind == session.constants.result_kinds.VOID then
    result = {
      type = "VOID"
    }
  elseif result_kind == session.constants.result_kinds.ROWS then
    local metadata = parse_metadata(session, response.buffer)
    result = parse_rows(session, response.buffer, metadata)
    result.type = "ROWS"
    result.meta = {
      has_more_pages = metadata.has_more_pages,
      paging_state = metadata.paging_state
    }
  elseif result_kind == session.constants.result_kinds.SET_KEYSPACE then
    result = {
      type = "SET_KEYSPACE",
      keyspace = session.unmarshaller.read_string(response.buffer)
    }
  elseif result_kind == session.constants.result_kinds.SCHEMA_CHANGE then
    result = {
      type = "SCHEMA_CHANGE",
      change = session.unmarshaller.read_string(response.buffer),
      keyspace = session.unmarshaller.read_string(response.buffer),
      table = session.unmarshaller.read_string(response.buffer)
    }
  else
    return string.format("Invalid result kind: %x", result_kind)
  end

  if response.tracing_id then
    result.tracing_id = response.tracing_id
  end

  return result
end

return _M
