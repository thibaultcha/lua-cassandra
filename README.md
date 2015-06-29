# lua-cassandra ![Module Version][badge-version-image]

> This project is a fork of [jbochi/lua-resty-cassandra][lua-resty-cassandra]. It adds support for binary protocol v3, a few bug fixes and more to come. See the [improvements][improvements-anchor] section for more details.

[![Build Status][badge-travis-image]][badge-travis-url]
[![Coverage Status][badge-coveralls-image]][badge-coveralls-url]

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

You can check more examples in the [tests](https://github.com/thibaultcha/lua-cassandra/blob/master/spec/integration_spec.lua) or [here][anchor-examples].

## Documentation

Coming soon.

## Examples

Coming soon.

## Improvements from root project

- [x] Support for binary protocol v3
  - [x] User Defined Types and Tuples support
  - [x] Serial Consistency support for batch requests
- [x] Keyspace switch fix
– [x] IPv6 encoding fix

## Roadmap

- [ ] Support for authentication
- [ ] Support for binary protocol v3 named values when binding a query
- [ ] Support for binary protocol v3 default timestamp option

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

[luarocks-url]: https://luarocks.org
[anchor-examples]: #examples
[lua-resty-cassandra]: https://github.com/jbochi/lua-resty-cassandra

[badge-travis-url]: https://travis-ci.org/thibaultcha/lua-cassandra
[badge-travis-image]: https://img.shields.io/travis/thibaultcha/lua-cassandra.svg?style=flat

[badge-coveralls-url]: https://coveralls.io/r/thibaultcha/lua-cassandra?branch=master
[badge-coveralls-image]: https://coveralls.io/repos/thibaultcha/lua-cassandra/badge.svg?branch=master&style=flat

[badge-version-image]: https://img.shields.io/badge/version-0.5--7-green.svg?style=flat
