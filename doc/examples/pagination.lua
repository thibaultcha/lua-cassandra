--------------------
-- manual pagination
--------------------

local cassandra = require "cassandra"

local client = assert(cassandra.new {
  host = "127.0.0.1",
  keyspace = "my_keyspace"
})

client:settimeout(1000)

assert(client:connect())

-- assume 1190 rows in users table

-- 1st page
local rows_1 = assert(client:execute("SELECT * FROM users"))
print(#rows_1)                    -- 1000 (default page_size)
print(rows_1.meta.has_more_pages) -- true

-- 2nd page
local rows_2 = assert(client:execute("SELECT * FROM users", nil, {
  page_size = 100,
  paging_state = rows_1.meta.paging_state
}))
print(#rows_2)                    -- 100
print(rows_2.meta.has_more_pages) -- true

-- 3rd, last page
local rows_3 = assert(client:execute("SELECT * FROM users", nil, {
  page_size = 100,
  paging_state = rows_2.meta.paging_state
}))
print(#rows_3)                    -- 90
print(rows_3.meta.has_more_pages) -- false

-----------------------
-- automated pagination
-----------------------

for rows, err, page in client:iterate("SELECT * FROM users") do
  if err then
    error(err)
  end
  print(page)
  print(#rows)
end
