--------
-- Pagination might be very useful to build web services.
-- Assuming the users table contains 1000 rows:

local cassandra = require "cassandra"

local session, err = cassandra.spawn_session {
  shm = "cassandra", -- used to store cluster infos
  contact_points = {"127.0.0.1", "127.0.0.2", "127.0.0.3"}, -- entry points to your cluster
  keyspace = "my_keyspace", -- this keyspace must exist
  query_options = {
    page_size = 500 -- default is 1000, reduced to 500 for this session
  }
}
assert(err == nil)

local select_query = "SELECT * FROM users"

-- 1st page
local rows, err = session:execute(select_query) -- using the session page_size
assert(#rows == 500) -- rows contains the 500 first rows
assert(rows.meta.has_more_pages) -- true when the column family contains more rows than fetched

-- 2nd page
rows, err = session:execute(select_query, nil {
  page_size = 100 -- override the session page_size for this query only
  paging_state = rows.meta.paging_state
})
assert(#rows == 100)
assert(rows.meta.has_more_mages)

-- 3rd page
rows, err = session:execute(select_query, nil, {
  paging_state = rows.meta.paging_state
})
assert(#rows == 400) -- last 400 rows
assert(rows.meta.has_more_pages == false)

session:shutdown()

--------
-- Automated pagination.
-- Assuming our users table now contains 10.000 rows:

for rows, err, page in session:execute("SELECT * FROM users", nil, {auto_paging = true}) do
  assert.same(500, #rows) -- rows contains 500 rows on each iteration in this case
  -- err: not nil if any fetch returns an error, this will be the last iteration
  -- page: will be 1 on the first iteration, 2 on the second, etc.
end
