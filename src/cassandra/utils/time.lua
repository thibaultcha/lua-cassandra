local type = type
local exec = os.execute
local ngx_sleep
local is_ngx = ngx ~= nil
if is_ngx then
  ngx_sleep = ngx.sleep
end

local function get_time()
  if ngx and type(ngx.now) == "function" then
    return ngx.now() * 1000
  else
    return os.time() * 1000
  end
end

local function wait(t)
  if t == nil then t = 0.5 end
  if is_ngx then
    ngx_sleep(t)
  else
    exec("sleep "..t)
  end
end

return {
  get_time = get_time,
  wait = wait
}
