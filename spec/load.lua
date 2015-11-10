package.path = package.path..";src/?.lua"
local Client = require "cassandra.client"

local client = Client({contact_points = {"127.0.0.1", "127.0.0.2"}, print_log_level = "INFO"})

for i = 1, 10000 do
  local res, err = client:execute("SELECT peer FROM system.peers")
  if err then
    error(err)
  end
  print("Request "..i.." successful.")
end

client:shutdown()
