# lua-cassandra

![Module Version][badge-version-image]
[![Build Status][badge-travis-image]][badge-travis-url]
[![Coverage Status][badge-coveralls-image]][badge-coveralls-url]

A pure Lua client library for Apache Cassandra (2.x/3.x), compatible with
[OpenResty].

## Table of Contents

- [Features](#features)
- [Usage](#usage)
- [Installation](#installation)
- [Documentation and Examples](#documentation-and-examples)
- [Roadmap](#roadmap)
- [Development](#development)

## Features

This library offers 2 modules: a "single host" module, compatible with PUC Lua 5.1/5.2,
LuaJIT and OpenResty, which allows your application to connect itself to a
given Cassandra node, and a "cluster" module, only compatible with OpenResty
which adds support for multi-node Cassandra datacenters.

- Single host `cassandra` module:
  - no dependencies
  - support for Cassandra 2.x and 3.x
  - simple, prepared, and batch statements
  - pagination (manual and automatic via Lua iterators)
  - SSL client-to-node connections
  - client authentication
  - leverage the non-blocking, reusable cosocket API in ngx_lua (with
    automatic fallback to LuaSocket in non-supported contexts)

- Cluster `resty.cassandra.cluster` module:
  - all features from the `cassandra` module
  - cluster topology discovery
  - advanced querying options
  - configurable policies (load balancing, retry, reconnection)
  - optimized performance for OpenResty

[Back to TOC](#table-of-contents)

## Usage

Single host module (Lua and OpenResty):

```lua
local cassandra = require "cassandra"

local peer = assert(cassandra.new {
  host = "127.0.0.1",
  port = 9042,
  keyspace = "my_keyspace"
})

peer:settimeout(1000)

assert(peer:connect())

assert(peer:execute("INSERT INTO users(id, name, age) VALUES(?, ?, ?)", {
  cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11"),
  "John O Reilly",
  42
}))

local rows = assert(peer:execute "SELECT * FROM users")

local user = rows[1]
print(user.name) -- John O Reilly
print(user.age)  -- 42

peer:close()
```

Cluster module (OpenResty only):

```
http {
    # you do not need the following line if you are using luarocks
    lua_package_path "/path/to/src/?.lua;/path/to/src/?/init.lua;;";

    # all cluster informations will be stored here
    lua_shared_dict cassandra 1m;

    server {
        ...

        location / {
            content_by_lua_block {
                local Cluster = require 'resty.cassandra.cluster'

                -- For performance reasons, the cluster variable
                -- should live in an upvalue at the main chunk level of your
                -- modules to avoid creating it on every request.
                -- see the 'intro' example in the online documentation.
                local cluster, err = Cluster.new {
                    shm = 'cassandra', -- defined by the lua_shared_dict directive
                    contact_points = {'127.0.0.1', '127.0.0.2'},
                    keyspace = 'my_keyspace'
                }
                if not cluster then
                    ngx.log(ngx.ERR, 'could not create cluster: ', err)
                    return ngx.exit(500)
                end

                local rows, err = cluster:execute "SELECT * FROM users"
                if not rows then
                    ngx.log(ngx.ERR, 'could not retrieve users: ', err)
                    return ngx.exit(500)
                end

                ngx.say('users: ', #rows)
            }
        }
    }
}
```

[Back to TOC](#table-of-contents)

## Installation

With [Luarocks]:

```bash
$ luarocks install lua-cassandra
```

Or via [opm](https://github.com/openresty/opm):

```
$ opm get thibaultcha/lua-cassandra
```

Or manually:

Once you have a local copy of this module's `lib/` directory, add it to your
`LUA_PATH` (or `lua_package_path` directive for OpenResty):

```
/path/to/lib/?.lua;/path/to/lib/?/init.lua;
```

**Note**: When used *outside* of OpenResty, or in the `init_by_lua` context,
this module requires additional dependencies:

- [LuaSocket](http://w3.impa.br/~diego/software/luasocket/)
- If you wish to use SSL client-to-node connections,
  [LuaSec](https://github.com/brunoos/luasec)
- When used in PUC-Lua,
  [Lua BitOp](http://bitop.luajit.org/) (installed by Luarocks)

[Back to TOC](#table-of-contents)

## Documentation and Examples

Refer to the online [manual] and detailed [documentation]. You will also find
[examples] there and you can browse the test suites for in-depth ones.

[Back to TOC](#table-of-contents)

## Roadmap

Cluster:
- new load balancing policies (token-aware)

CQL:
- implement `decimal` data type
- v4: implement `date` and `time` data types
- v4: implement `smallint` and `tinyint` data types

[Back to TOC](#table-of-contents)

## Development

#### Test Suites

The single host tests require [busted] and [ccm] to be installed. They can be
run with:

```
$ make busted
```

The cluster module tests require
[Test::Nginx::Socket](http://search.cpan.org/~agent/Test-Nginx-0.23/lib/Test/Nginx/Socket.pm)
in addition to ccm. They can be run with:

```
$ make prove
```

#### Tools

This module uses various tools for documentation and code quality, they can
easily be installed from Luarocks by running:

```
$ make dev
```

Code coverage is analyzed with [luacov](http://keplerproject.github.io/luacov/)
from the **busted** tests:

```
$ make coverage
```

The code is linted with [luacheck](https://github.com/mpeterv/luacheck):

```
$ make lint
```

The documentation is generated with
[ldoc](https://github.com/stevedonovan/LDoc) and can be generated with:

```
$ make doc
```

[Back to TOC](#table-of-contents)

[Luarocks]: https://luarocks.org
[OpenResty]: https://openresty.org
[ccm]: https://github.com/pcmanus/ccm
[busted]: http://olivinelabs.com/busted

[documentation]: http://thibaultcha.github.io/lua-cassandra/
[manual]: http://thibaultcha.github.io/lua-cassandra/manual/README.md.html
[examples]: http://thibaultcha.github.io/lua-cassandra/examples/intro.lua.html

[badge-travis-url]: https://travis-ci.org/thibaultcha/lua-cassandra
[badge-travis-image]: https://travis-ci.org/thibaultcha/lua-cassandra.svg?branch=master

[badge-coveralls-url]: https://coveralls.io/r/thibaultcha/lua-cassandra?branch=master
[badge-coveralls-image]: https://coveralls.io/repos/thibaultcha/lua-cassandra/badge.svg?branch=master&style=flat

[badge-version-image]: https://img.shields.io/badge/version-1.5.2-blue.svg?style=flat
