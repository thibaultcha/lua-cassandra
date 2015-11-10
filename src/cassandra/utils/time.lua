local type = type

local function get_time()
  if ngx and type(ngx.now) == "function" then
    return ngx.now() * 1000
  else
    return os.time() * 1000
  end
end

return {
  get_time = get_time
}
