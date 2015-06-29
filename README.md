# lua-cassandra

> This project is a fork of [jbochi/lua-resty-cassandra][lua-resty-cassandra]. It adds support for binary protocol v3, a few bug fixes and more to come.

Pure Lua Cassandra client using CQL binary protocol v2/v3.

It is 100% non-blocking if used in Nginx/Openresty but can also be used with luasocket.

## Installation

#### Luarocks

Installation through [luarocks][luarocks-url] is recommended:

```bash
$ luarocks install lua-cassandra
```

#### Manual

Copy the `src/` folder and require `cassandra.lua`.

## Usage

```lua
local cassandra = require "cassandra"
-- local cassandra = require "cassandra.v2" -- binary protocol v2 for Cassandra 2.0.x

local session = cassandra:new()
session:set_timeout(1000) -- 1000ms timeout

local connected, err = session:connect("127.0.0.1", 9042)
assert(connected)
session:set_keyspace("demo")

-- simple query
local table_created, err = session:execute [[
  CREATE TABLE users(
    id uuid PRIMARY KEY,
    name varchar,
    age int
  )
]]

-- query with arguments
local ok, err = session:execute("INSERT INTO users(name, age, user_id) VALUES(?, ?, ?)"
  , {"John O'Reilly", 42, cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11")})


-- select statement
local users, err = session:execute("SELECT name, age, user_id FROM users")
assert(1 == #users)

local user = users[1]
print(user.name) -- "John O'Reilly"
print(user.user_id) -- "1144bada-852c-11e3-89fb-e0b9a54a6d11"
print(user.age) -- 42
```

You can check more examples in the [tests](https://github.com/thibaultcha/lua-cassandra/blob/master/spec/integration_spec.lua) or [here][anchor-examples].

## Documentation

Coming soon.

## Examples

Batches:

```lua
-- Create a batch statement
local batch = cassandra:BatchStatement()

-- Add a query
batch:add("INSERT INTO users (name, age, user_id) VALUES (?, ?, ?)",
          {"James", 32, cassandra.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93")})

-- Add a prepared statement
local stmt, err = session:prepare("INSERT INTO users (name, age, user_id) VALUES (?, ?, ?)")
batch:add(stmt, {"John", 45, cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11")})

-- Execute the batch
local result, err = session:execute(batch)
```

Pagination might be very useful to build web services:

```lua
-- Assuming our users table contains 1000 rows

local query = "SELECT * FROM users"
local rows, err = session:execute(query, nil, {page_size = 500}) -- default page_size is 5000

assert.same(500, #rows) -- rows contains the 500 first rows

if rows.meta.has_more_pages then
  local next_rows, err = session:execute(query, nil, {paging_state = rows.meta.paging_state})

  assert.same(500, #next_rows) -- next_rows contains the next (and last) 500 rows
end
```

Automated pagination:

```lua
-- Assuming our users table now contains 10.000 rows

local query = "SELECT * FROM users"

for rows, err, page in session:execute(query, nil, {auto_paging = true}) do
  assert.same(5000, #rows) -- rows contains 5000 rows on each iteration in this case
  -- err: in case any fetch returns an error
  -- page: will be 1 on the first iteration, 2 on the second, etc.
end
```

## Roadmap

- [] Support for authentication
- [] Support for binary protocol v3 named values when binding a query
- [] Support for binary protocol v3 default timestamp option

## Running unit tests

We use `busted` and require `luasocket` to mock `ngx.socket.tcp()`. To run the tests, start a local cassandra instance and run:

```bash
$ make dev
$ make test
```

This will run tests for both binary protocol v2 and v3, so you must ensure to be running Cassandra `2.1.x`.

## Running coverage

```bash
$ make dev
$ make coverage
```

Report will be in `./luacov.report.out`.

## Running linting

```bash
$ make dev
$ make lint
```

[lua-resty-cassandra]: https://github.com/jbochi/lua-resty-cassandra
[luarocks-url]: https://luarocks.org
[anchor-examples]: #examples
