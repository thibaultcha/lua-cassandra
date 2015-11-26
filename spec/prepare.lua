package.path = "src/?.lua;src/?/init.lua;"..package.path
local inspect = require "inspect"
local cassandra = require "cassandra"
local log = require "cassandra.log"

log.set_lvl("ERR")

local _, err, cluster = cassandra.spawn_cluster {shm = "cassandra", contact_points = {"127.0.0.1"}}
assert(err == nil, inspect(err))

local session, err = cluster:spawn_session({keyspace = "page"})
assert(err == nil, inspect(err))

-- for i = 1, 10000 do
--   local res, err = session:execute("INSERT INTO users(id, name, age) VALUES(uuid(), ?, ?)", {"Alice", i})
--   if err then
--     error(err)
--   end
-- end

local start, total

start = os.time()
for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = 20, auto_paging = true}) do

end

total = os.time() - start
print("Time without prepared = "..total)

start = os.time()
for rows, err, page in session:execute("SELECT * FROM users", nil, {page_size = 20, auto_paging = true, prepare = true}) do

end

total = os.time() - start
print("Time with prepared = "..total)
