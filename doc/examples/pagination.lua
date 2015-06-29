--------
-- Pagination might be very useful to build web services.
-- Assuming our users table contains 1000 rows:

local query = "SELECT * FROM users"
local rows, err = session:execute(query, nil, {page_size = 500}) -- default page_size is 5000

assert.same(500, #rows) -- rows contains the 500 first rows

if rows.meta.has_more_pages then
  local next_rows, err = session:execute(query, nil, {paging_state = rows.meta.paging_state})

  assert.same(500, #next_rows) -- next_rows contains the next (and last) 500 rows
end

--------
-- Automated pagination.
-- Assuming our users table now contains 10.000 rows:

local query = "SELECT * FROM users"

for rows, err, page in session:execute(query, nil, {auto_paging = true}) do
  assert.same(5000, #rows) -- rows contains 5000 rows on each iteration in this case
  -- err: in case any fetch returns an error
  -- page: will be 1 on the first iteration, 2 on the second, etc.
end
