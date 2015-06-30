# lua-cassandra ![Module Version][badge-version-image] [![Build Status][badge-travis-image]][badge-travis-url] [![Coverage Status][badge-coveralls-image]][badge-coveralls-url]

> This project is a fork of [jbochi/lua-resty-cassandra][lua-resty-cassandra]. It adds support for binary protocol v3, a few bug fixes and more to come. See the improvements section for more details.

Lua Cassandra client using CQL binary protocol v2/v3.

It is 100% non-blocking if used in Nginx/Openresty but can also be used with luasocket.

## Installation

#### Luarocks

Installation through [luarocks][luarocks-url] is recommended:

```bash
$ luarocks install lua-cassandra
```

#### Manual

Simply copy the `src/` folder in your application.

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

You can check more examples on the [documentation][documentation-reference] or in the [tests](https://github.com/thibaultcha/lua-cassandra/blob/master/spec/integration_spec.lua).

## Documentation and examples

Refer to the online [manual][documentation-manual] and [reference][documentation-reference].

## Improvements

This fork provides the following improvements over the root project:

- [x] Support for binary protocol v3
  - [x] User Defined Types and Tuples support
  - [x] Serial Consistency support for batch requests
- [x] Keyspace switch fix
- [x] IPv6 encoding fix

## Roadmap

- [ ] Support for authentication
- [ ] Support for binary protocol v3 named values when binding a query
- [ ] Support for binary protocol v3 default timestamp option

## Makefile Operations

When developing, use the `Makefile` for doing the following operations:

| Name          | Description                                   |
| -------------:| ----------------------------------------------|
| `dev`         | Install busted, luacov and luacheck           |
| `test`        | Run the unit tests                            |
| `lint`        | Lint all Lua files in the repo                |
| `coverage`    | Run unit tests + coverage report              |
| `clean`       | Clean coverage report                         |

**Note:** Before running `make lint` or `make test` you will need to run `make dev`.

**Note bis:** Tests are running for both binary protocol v2 and v3, so you must ensure to be running Cassandra `2.1.x`.

[luarocks-url]: https://luarocks.org
[lua-resty-cassandra]: https://github.com/jbochi/lua-resty-cassandra
[documentation-reference]: http://thibaultcha.github.io/lua-cassandra/
[documentation-manual]: http://thibaultcha.github.io/lua-cassandra/manual/README.md.html

[badge-travis-url]: https://travis-ci.org/thibaultCha/lua-cassandra
[badge-travis-image]: https://img.shields.io/travis/thibaultCha/lua-cassandra.svg?style=flat

[badge-coveralls-url]: https://coveralls.io/r/thibaultCha/lua-cassandra?branch=master
[badge-coveralls-image]: https://coveralls.io/repos/thibaultCha/lua-cassandra/badge.svg?branch=master&style=flat

[badge-version-image]: https://img.shields.io/badge/version-0.5--7-blue.svg?style=flat
