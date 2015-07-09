local Reader_v2 = require "cassandra.protocol.reader_v2"

local _M = Reader_v2:extend()

function _M:receive_frame(session)
  local unmarshaller = self.unmarshaller

  local header, err = session.socket:receive(9)
  if not header then
    return nil, string.format("Failed to read frame header from %s: %s", session.host, err)
  end
  local header_buffer = unmarshaller.create_buffer(header)
  local version = unmarshaller.read_raw_byte(header_buffer)
  if version ~= self.constants.version_codes.RESPONSE then
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
    buffer = body_buffer
  }
end

function _M:parse_column_type(buffer, column_name)
  local column_type = self.unmarshaller.read_option(buffer)
  -- Decode UDTs and Tuples
  if self.unmarshaller.type_decoders[column_type.id] then
    column_type = self.unmarshaller.type_decoders[column_type.id](buffer, column_type, column_name)
  end
  return column_type
end

local constants = require "cassandra.constants.constants_v3"
_M.result_kind_parsers = {
  [constants.result_kinds.SCHEMA_CHANGE] = function(self, buffer)
    local change_type = self.unmarshaller.read_string(buffer)
    local target = self.unmarshaller.read_string(buffer)
    local ksname = self.unmarshaller.read_string(buffer)
    local tablename, user_type_name
    if target == "TABLE" then
      tablename = self.unmarshaller.read_string(buffer)
    elseif target == "TYPE" then
      user_type_name = self.unmarshaller.read_string(buffer)
    end

    return {
      type = "SCHEMA_CHANGE",
      change = self.unmarshaller.read_string(buffer),
      keyspace = self.unmarshaller.read_string(buffer),
      table = self.unmarshaller.read_string(buffer),
      change_type = change_type,
      target = target,
      keyspace = ksname,
      table = tablename,
      user_type = user_type_name
     }
  end
}

return _M
