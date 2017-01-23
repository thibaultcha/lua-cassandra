# lua-cassandra ![Module Version][badge-version-image] [![Build Status][badge-travis-image]][badge-travis-url] [![Coverage Status][badge-coveralls-image]][badge-coveralls-url]

A pure Lua client library for Apache Cassandra, compatible with [ngx_lua]/[OpenResty] and plain Lua.

It is implemented following the example of the official Datastax drivers, and tries to offer the same behaviors, options and features.

## Table of Contents

- [Features](#features)
- [Usage](#usage)
- [Installation](#installation)
- [Documentation and Examples](#documentation-and-examples)
- [Roadmap](#roadmap)
- [Test Suites](#test-suites)
- [Tools](#tools)

## Features

- Leverage the ngx_lua cosocket API (non-blocking, reusable sockets)
- Fallback on LuaSocket for plain Lua compatibility
- Simple, prepared and batch statements
- Cluster topology automatic discovery
- Configurable load balancing, reconnection and retry policies
- TLS client-to-node encryption
- Client authentication
- Highly configurable options per session/query
- Support Cassandra 2.0+
- Compatible with Lua 5.1, 5.2, 5.3, LuaJIT 2.x, and optimized for OpenResty/ngx_lua.

## Usage

With ngx_lua:

```nginx
http {
  # you do not need the following line if you are using luarocks
  lua_package_path "/path/to/src/?.lua;/path/to/src/?/init.lua;;";

  # all cluster informations will be stored here
  lua_shared_dict cassandra 1m;

  server {
    ...

    location / {
      content_by_lua '
        local cassandra = require "cassandra"

        local session, err = cassandra.spawn_session {
          shm = "cassandra", -- defined by "lua_shared_dict"
          contact_points = {"127.0.0.1"}
        }
        if err then
          ngx.log(ngx.ERR, "Could not spawn session: ", tostring(err))
          return ngx.exit(500)
        end

        local res, err = session:execute("INSERT INTO users(id, name, age) VALUES(?, ?, ?)", {
          cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11"),
          "John O Reilly",
          42
        })
        if err then
          -- ...
        end

        local rows, err = session:execute("SELECT * FROM users")
        if err then
          -- ...
        end

        session:set_keep_alive()

        ngx.say("rows retrieved: ", #rows)
      ';
    }
  }
}
```

With plain Lua:

```lua
local cassandra = require "cassandra"

local session, err = cassandra.spawn_session {
  shm = "cassandra",
  contact_points = {"127.0.0.1", "127.0.0.2"}
}
assert(err == nil)

local res, err = session:execute("INSERT INTO users(id, name, age) VALUES(?, ?, ?)", {
  cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11"),
  "John O'Reilly",
  42
})
assert(err == nil)

local rows, err = session:execute("SELECT * FROM users")
assert(err == nil)

print("rows retrieved: ", #rows)

session:shutdown()
```

## Installation

With [Luarocks]:

```bash
$ luarocks install lua-cassandra
```

Manually:

Once you have a local copy of this module's `src/` directory, add it to your `LUA_PATH` (or `lua_package_path` directive for ngx_lua):

```
/path/to/src/?.lua;/path/to/src/?/init.lua;
```

**Note**: When used *outside* of ngx_lua, this module requires:

- [LuaSocket](http://w3.impa.br/~diego/software/luasocket/)
- If you wish to use TLS client-to-node encryption, [LuaSec](https://github.com/brunoos/luasec)

## Documentation and Examples

Refer to the online [manual] and detailed [documentation]. You will also find [examples] there and you can browse the test suites for in-depth ones.

## Roadmap

CQL:
- Support for query tracing
- Support for native protocol v3's default timestamps and named parameters
- Support for native protocol v4

Documentation:
- Options
- Errors
- Type inference of binded parameters
- Type serialization example

## Test Suites

This library relies on three test suites:

- Unit tests, with busted
- Integration tests, with busted and [ccm]
- ngx_lua integration tests with Test::Nginx::Socket and a running Cassandra cluster

The first can simply be run after installing [busted] and running:

```shell
$ busted spec/01-unit
```

The integration tests are located in another folder, and require [ccm] to be installed.

```shell
busted spec/02-integration
```

Finally, the ngx_lua integration tests can be run after installing the [Test::Nginx::Socket](http://search.cpan.org/~agent/Test-Nginx-0.23/lib/Test/Nginx/Socket.pm) module and require a Cassandra instance to be running on `localhost`:

```shell
$ prove t/
```

## Tools

This module can also use various tools for documentation and code quality, they can easily be installed from Luarocks by running:

```
$ make dev
```

Code coverage is analyzed by [luacov](http://keplerproject.github.io/luacov/) from the **busted** (unit and integration) tests:

```shell
$ busted --coverage
$ luacov cassandra
# or
$ make coverage
```

The code is linted with [luacheck](https://github.com/mpeterv/luacheck). It is easier to use the Makefile again to avoid analyzing Lua files that are not part of this module:

```shell
$ make lint
```

The documentation is generated by [ldoc](https://github.com/stevedonovan/LDoc) and can be generated with:

```shell
$ ldoc -c doc/config.ld src
# or
$ make doc
```

[Luarocks]: https://luarocks.org
[OpenResty]: https://openresty.org
[ccm]: https://github.com/pcmanus/ccm
[busted]: http://olivinelabs.com/busted
[ngx_lua]: https://github.com/openresty/lua-nginx-module

[documentation]: http://thibaultcha.github.io/lua-cassandra/
[manual]: http://thibaultcha.github.io/lua-cassandra/manual/README.md.html
[examples]: http://thibaultcha.github.io/lua-cassandra/examples/basic.lua.html

[badge-travis-url]: https://travis-ci.org/thibaultCha/lua-cassandra
[badge-travis-image]: https://travis-ci.org/thibaultCha/lua-cassandra.svg?branch=master

[badge-coveralls-url]: https://coveralls.io/r/thibaultCha/lua-cassandra?branch=master
[badge-coveralls-image]: https://coveralls.io/repos/thibaultCha/lua-cassandra/badge.svg?branch=master&style=flat

[badge-version-image]: https://img.shields.io/badge/version-0.5.5-blue.svg?style=flat
