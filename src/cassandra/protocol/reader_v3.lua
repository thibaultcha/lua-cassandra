local utils = require "cassandra.utils"
local reader_v2 = require "cassandra.protocol.reader_v2"

local _M = utils.deep_copy(reader_v2)

function _M.receive_frame(session)
  local unmarshaller = session.unmarshaller

  local header, err = session.socket:receive(9)
  if not header then
    return nil, string.format("Failed to read frame header from %s: %s", session.host, err)
  end
  local header_buffer = unmarshaller.create_buffer(header)
  local version = unmarshaller.read_raw_byte(header_buffer)
  if version ~= session.constants.version_codes.RESPONSE then
    return nil, string.format("Invalid response version received from %s", session.host)
  end
  local flags = unmarshaller.read_raw_byte(header_buffer)
  local stream = unmarshaller.read_short(header_buffer)
  local op_code = unmarshaller.read_raw_byte(header_buffer)
  local length = unmarshaller.read_int(header_buffer)

  local body
  if length > 0 then
    body, err = session.socket:receive(length)
    if not body then
      return nil, string.format("Failed to read frame body from %s: %s", session.host, err)
    end
  else
    body = ""
  end

  local body_buffer = unmarshaller.create_buffer(body)
  return {
    flags = flags,
    stream = stream,
    op_code = op_code,
    body = body, -- TODO remove
    buffer = body_buffer
  }
end

function _M.parse_metadata(session, buffer)
  local unmarshaller = session.unmarshaller
  -- Flags parsing
  local flags = unmarshaller.read_int(buffer)
  local global_tables_spec = utils.hasbit(flags, session.constants.rows_flags.GLOBAL_TABLES_SPEC)
  local has_more_pages = utils.hasbit(flags, session.constants.rows_flags.HAS_MORE_PAGES)
  local columns_count = unmarshaller.read_int(buffer)

  -- Potential paging metadata
  local paging_state
  if has_more_pages then
    paging_state = unmarshaller.read_bytes(buffer)
  end

  -- Potential global_tables_spec metadata
  local global_keyspace_name, global_table_name
  if global_tables_spec then
    global_keyspace_name = unmarshaller.read_string(buffer)
    global_table_name = unmarshaller.read_string(buffer)
  end

  -- Columns metadata
  local columns = {}
  for _ = 1, columns_count do
    local ksname = global_keyspace_name
    local tablename = global_table_name
    if not global_tables_spec then
      ksname = unmarshaller.read_string(buffer)
      tablename = unmarshaller.read_string(buffer)
    end
    local column_name = unmarshaller.read_string(buffer)
    local column_type = unmarshaller.read_option(buffer)

    -- Decode UDTs and Tuples
    if unmarshaller.type_decoders[column_type.id] then
      column_type = unmarshaller.type_decoders[column_type.id](buffer, column_type, column_name)
    end

    columns[#columns + 1] = {
      keyspace = ksname,
      table = tablename,
      name = column_name,
      type = column_type
    }
  end

  return {
    columns = columns,
    paging_state = paging_state,
    columns_count = columns_count,
    has_more_pages = has_more_pages
  }
end

function _M.parse_response(session, response)
  local result, tracing_id

  -- Check if frame is an error
  if response.op_code == session.constants.op_codes.ERROR then
    return nil, _M.read_error(session, response.buffer)
  end

  if response.flags == session.constants.flags.TRACING then -- tracing
    tracing_id = session.unmarshaller.read_uuid(string.sub(response.body, 1, 16))
    response.buffer.pos = 17
  end

  local result_kind = session.unmarshaller.read_int(response.buffer)

  if result_kind == session.constants.result_kinds.VOID then
    result = {
      type = "VOID"
    }
  elseif result_kind == session.constants.result_kinds.ROWS then
    local metadata = _M.parse_metadata(session, response.buffer)
    result = _M.parse_rows(session, response.buffer, metadata)
    result.type = "ROWS"
    result.meta = {
      has_more_pages = metadata.has_more_pages,
      paging_state = metadata.paging_state
    }
  elseif result_kind == session.constants.result_kinds.PREPARED then
    local id = session.unmarshaller.read_short_bytes(response.buffer)
    local metadata = _M.parse_metadata(session, response.buffer)
    local result_metadata = _M.parse_metadata(session, response.buffer)
    assert(response.buffer.pos == #(response.buffer.str) + 1)
    result = {
      id = id,
      type = "PREPARED",
      metadata = metadata,
      result_metadata = result_metadata
    }
  elseif result_kind == session.constants.result_kinds.SET_KEYSPACE then
    result = {
      type = "SET_KEYSPACE",
      keyspace = session.unmarshaller.read_string(response.buffer)
    }
  elseif result_kind == session.constants.result_kinds.SCHEMA_CHANGE then
    local change_type = session.unmarshaller.read_string(response.buffer)
    local target = session.unmarshaller.read_string(response.buffer)
    local ksname = session.unmarshaller.read_string(response.buffer)
    local tablename, user_type_name
    if target == "TABLE" then
      tablename = session.unmarshaller.read_string(response.buffer)
    elseif target == "TYPE" then
      user_type_name = session.unmarshaller.read_string(response.buffer)
    end

    result = {
      type = "SCHEMA_CHANGE",
      change = session.unmarshaller.read_string(response.buffer),
      keyspace = session.unmarshaller.read_string(response.buffer),
      table = session.unmarshaller.read_string(response.buffer),
      change_type = change_type,
      target = target,
      keyspace = ksname,
      table = tablename,
      user_type = user_type_name
     }
  else
    return nil, string.format("Invalid result kind: %x", result_kind)
  end

  result.tracing_id = tracing_id

  return result
end

return _M
