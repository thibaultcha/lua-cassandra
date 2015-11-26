local exec = os.execute
local time = os.time

local sleep
local now
local ngx_get_phase
local is_ngx = ngx ~= nil
if is_ngx then
  sleep = ngx.sleep
  now = ngx.now
  ngx_get_phase = ngx.get_phase
end

local function get_time()
  if is_ngx and ngx_get_phase() ~= "init" then
    return now() * 1000
  else
    return time() * 1000
  end
end

local function wait(t)
  if t == nil then t = 0.5 end

  if is_ngx then
    local phase = ngx_get_phase()
    if phase == "rewrite" or phase == "access" or phase == "content" then
      sleep(t)
      return
    end
  end

  exec("sleep "..t)
end

return {
  get_time = get_time,
  wait = wait
}
