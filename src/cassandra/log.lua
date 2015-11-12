local ngx_log = ngx.log

local log = {}

for _, lvl in ipairs({"ERR", "WARN", "INFO", "DEBUG"}) do
  log[lvl:lower()] = function(...)
    if ngx ~= nil and ngx.get_phase() ~= "init" then
      ngx_log(ngx[lvl], ...)
    else
      print(...)
    end
  end
end

return log
