--------
-- Every error is represented by a table with the following properties.
-- @module Error

--- A description of any error returned by the library.
-- @field code The error code for the error. `-1` means the error comes from the client.
-- @field message A formatted error message (with the translated error code if is a Cassandra error).
-- @field raw_message The raw error message as returned by Cassandra.
-- @table error

local error_mt = {}

error_mt = {
  __tostring = function(self)
    return self.message
  end,
  __concat = function(a, b)
    if getmetatable(a) == error_mt then
      return a.message..b
    else
      return a..b.message
    end
  end,
  __call = function(self, message, raw_message, code)
    return setmetatable({
      code = code or -1,
      message = message,
      raw_message = raw_message or message
    }, error_mt)
  end
}

return setmetatable({}, error_mt)
