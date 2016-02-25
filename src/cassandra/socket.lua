local get_phase, ngx_socket, has_cosocket, log, warn

--- ngx_lua utils

if ngx ~= nil then
  log = ngx.log
  warn = ngx.WARN
  get_phase = ngx.get_phase
  ngx_socket = ngx.socket
  has_cosocket = function()
    local phase = get_phase()
    return phase == "rewrite" or phase == "access"
        or phase == "content" or phase == "timer", phase
  end
else
  log = function()end
  get_phase = function()end
  has_cosocket = function()end
end

--- LuaSocket proxy metatable

local luasocket_mt = {}

function luasocket_mt:__index(key)
  local override = rawget(luasocket_mt, key)
  if override ~= nil then
    return override
  end

  local orig = self.sock[key]
  if type(orig) == "function" then
    local f = function(_, ...)
      return orig(self.sock, ...)
    end
    self[key] = f
    return f
  end

  return orig
end


--- LuaSocket <-> ngx_lua compat

function luasocket_mt.getreusedtimes()
  return 0
end

function luasocket_mt:settimeout(t)
  self.sock:settimeout(t/1000)
end

function luasocket_mt:setkeepalive()
  self.sock:close()
  return true
end

--- Perform SSL handshake.
-- Mimics the ngx_lua `sslhandshake()` signature with an additional argument
-- to specify other SSL options for plain Lua.
function luasocket_mt:sslhandshake(reused_session, _, verify, luasec_opts)
  luasec_opts = luasec_opts or {}
  local return_bool = reused_session == false

  local ssl = require "ssl"
  local params = {
    mode = "client",
    protocol = "tlsv1",
    key = luasec_opts.key,
    certificate = luasec_opts.certificate,
    cafile = luasec_opts.ca,
    verify = verify and "peer" or "none",
    options = "all"
  }

  local ssl_sock, err = ssl.wrap(self.sock, params)
  if err then
    return return_bool and false or nil, err
  end

  local ok, err = ssl_sock:dohandshake()
  if not ok then
    return return_bool and false or nil, err
  end

  self.sock = ssl_sock

  return return_bool and true or ssl_sock
end

--- Module

return {
  tcp = function(...)
    local ok, phase = has_cosocket()
    if ok then
      return ngx_socket.tcp(...)
    elseif phase ~= "init" then
      log(warn, "no support for cosockets in this context, falling back on LuaSocket")
    end

    local socket = require "socket"

    return setmetatable({
      sock = socket.tcp(...)
    }, luasocket_mt)
  end,
  luasocket_mt = luasocket_mt,
  _VERSION = "0.0.2"
}
