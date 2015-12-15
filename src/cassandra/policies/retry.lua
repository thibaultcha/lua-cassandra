local DECISIONS = {
  throw = 0,
  retry = 1
}

local function on_unavailable(request_infos)
  return DECISIONS.throw
end

local function on_read_timeout(request_infos)
  if request_infos.n_retries > 0 then
    return DECISIONS.throw
  end

  return DECISIONS.retry
end

local function on_write_timeout(request_infos)
  if request_infos.n_retries > 0 then
    return DECISIONS.throw
  end

  return DECISIONS.retry
end

return {
  on_unavailable = on_unavailable,
  on_read_timeout = on_read_timeout,
  on_write_timeout = on_write_timeout,
  decisions = DECISIONS
}
