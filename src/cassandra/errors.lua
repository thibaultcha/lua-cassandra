local _ERRORS = {
  no_host = function(errors)
    if type(errors) == "string" then
      return errors
    end

    local buf = {}
    for address, err in pairs(errors) do
      buf[#buf + 1] = string.format("%s: %s.", address, err)
    end

    return "all hosts tried for query failed. "..table.concat(buf, " ")
  end,
  socket = function(peer, err)
    return "socket with peer '"..peer.."' encountered error: "..err
  end,
  shm = function(shm, err)
    return "shared dict '"..shm.."' encountered error: "..err
  end,
  internal_driver = function(err)
    return "internal driver error: "..err
  end,
  options = function(err)
    return "option error: "..err
  end
}

_ERRORS.t_cql = "cql"
_ERRORS.t_timeout = "timeout"
_ERRORS.t_socket = "socket"

return _ERRORS
