local decisions = {
  throw = 0,
  retry = 1
}

local _default = {}

function _default.on_unavailable(request_infos)
  return decisions.throw
end

function _default.on_read_timeout(request_infos)
  if request_infos.n_retries > 0 then
    return decisions.throw
  end

  return decisions.retry
end

function _default.on_write_timeout(request_infos)
  if request_infos.n_retries > 0 then
    return decisions.throw
  end

  return decisions.retry
end

return {
  default = _default,
  decisions = decisions
}
