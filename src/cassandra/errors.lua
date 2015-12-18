local type = type
local tostring = tostring
local string_format = string.format

--- CONST
-- @section constants

local ERROR_TYPES = {
  NoHostAvailableError = {
    info = "Represents an error when a query cannot be performed because no host is available or could be reached by the driver.",
    message = function(errors, msg)
      if type(errors) ~= "table" then
        return msg
      end

      local message = "All hosts tried for query failed."
      for address, err in pairs(errors) do
        message = string_format("%s %s: %s.", message, address, tostring(err))
      end
      return message
    end
  },
  ResponseError = {
    info = "Represents an error message from the server.",
    message = function(code, code_translation, message)
      return "["..code_translation.."] "..message
    end,
    meta = function(code)
      return {code = code}
    end
  },
  SocketError = {
    info = "Represents a client-side error that is raised when a socket returns an error from one of its operations.",
    message = function(address, message)
      return message.." for socket with peer "..address
    end
  },
  TimeoutError = {
    info = "Represents a client-side error that is raised when the client didn't hear back from the server within {client_options.socket_options.read_timeout}.",
    message = function(address)
      return "timeout for peer "..address
    end
  },
  AuthenticationError = {
    info = "Represents an authentication error from the driver or from a Cassandra node."
  },
  SharedDictError = {
    info = "Represents an error with the lua_shared_dict in use.",
    message = function(msg, shm)
      if shm ~= nil then
        return "shared dict "..shm.." returned error: "..msg
      else
        return msg
      end
    end,
    meta = function(message, shm)
      return {shm = shm}
    end
  },
  DriverError = {
    info = "Represents an error indicating the library is used in an erroneous way."
  }
}

--- ERROR_MT
-- @section error_mt

local _error_mt = {}
_error_mt.__index = _error_mt

function _error_mt:__tostring()
  return tostring(string_format("%s: %s", self.type, self.message))
end

function _error_mt.__concat(a, b)
  if getmetatable(a) == _error_mt then
    return tostring(a)..b
  else
    return a..tostring(b)
  end
end

--- _ERRORS
-- @section _errors

local _ERRORS = {}

local function build_error(k, v)
  return function(...)
    local arg = {...}
    local err = {
      type = k,
      info = v.info,
      message = type(v.message) == "function" and v.message(...) or arg[1]
    }

    if type(v.meta) == "function" then
      local meta = v.meta(...)
      for meta_k, meta_v in pairs(meta) do
        if err[meta_k] == nil then
          err[meta_k] = meta_v
        end
      end
    end

    return setmetatable(err, _error_mt)
  end
end

for k, v in pairs(ERROR_TYPES) do
  _ERRORS[k] = build_error(k, v)
end

if _G.test then
  return {
    error_mt = _error_mt,
    errors = _ERRORS,
    build_error = build_error
  }
else
  return _ERRORS
end
