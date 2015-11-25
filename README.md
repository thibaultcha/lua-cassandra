# lua-cassandra ![Module Version][badge-version-image] [![Build Status][badge-travis-image]][badge-travis-url] [![Coverage Status][badge-coveralls-image]][badge-coveralls-url]

A pure Lua client library for Apache Cassandra (2.0+), compatible with Lua and [ngx_lua].

It is build on the model of the official Datastax drivers, and tries to implement the same behaviors and features.

## Features

- Leverage the ngx_lua cosocket API (non-blocking, reusable sockets)
- Fallback on LuaSocket for plain Lua compatibility
- Simple, prepared and batch statements
- Cluster topology automatic discovery
- Configurable load balancing, reconnection and retry policies
- TLS client-to-node encryption
- Client authentication
- Highly configurable options per session/request
- Compatible with Cassandra 2.0 and 2.1

## Usage

With ngx_lua:

```nginx
http {
  # you do not need the following line if you are using
  # luarocks
  lua_package_path "/path/to/src/?.lua;/path/to/src/?/init.lua;;";

  # all cluster informations will be stored here
  lua_shared_dict cassandra 1m;

  init_by_lua '
    local cassandra = require "cassandra"

    -- retrieve cluster topology
    local ok, err = cassandra.spawn_cluster {
      shm = "cassandra", -- defined by "lua_shared_dict"
      contact_points = {"127.0.0.1", "127.0.0.2"}
    }
    if not ok then
      ngx.log(ngx.ERR, "Could not spawn cluster: ", err.message)
    end
  ';

  server {
    ...

    location /insert {
      local cassandra = require "cassandra"

      local session, err = cassandra.spawn_session {
        shm = "cassandra" -- defined by "lua_shared_dict"
      }
      if err then
        ngx.log(ngx.ERR, "Could not spawn session: ", err.message)
        return ngx.exit(500)
      end

      local res, err = session:execute("INSERT INTO users(id, name, age) VALUES(?, ?, ?)", {
        cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11"),
        "John O'Reilly",
        42
      })
      if err then
        -- ...
      end

      session:set_keep_alive()
    }

    location /get {
      content_by_lua '
        local cassandra = require "cassandra"

        local session, err = cassandra.spawn_session {
          shm = "cassandra" -- defined by "lua_shared_dict"
        }
        if err then
          ngx.log(ngx.ERR, "Could not spawn session: ", err.message)
          return ngx.exit(500)
        end

        local rows, err = session:execute("SELECT * FROM users")
        if err then
          -- ...
        end

        session:set_keep_alive()

        ngx.say("number of users: ", #rows)
      ';
    }
  }
}
```

With plain Lua:

```lua
local cassandra = require "cassandra"

local ok, err, cluster = cassandra.spawn_cluster {
  shm = "cassandra",
  contact_points = {"127.0.0.1", "127.0.0.2"}
}

local session, err = cluster:spawn_session()
assert(err == nil)

local res, err = session:execute("INSERT INTO users(id, name, age) VALUES(?, ?, ?)", {
  cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11"),
  "John O'Reilly",
  42
})
assert(err == nil)

local rows, err = session:execute("SELECT * FROM users")
assert(err == nil)

print("number of users: ", #rows)

session:shutdown()
```

## Installation

With [Luarocks]:

```bash
$ luarocks install lua-cassandra
```

If installed manually, this module requires:

- [cjson](https://github.com/mpx/lua-cjson/)
- [LuaSocket](http://w3.impa.br/~diego/software/luasocket/)
- If you wish to use TLS client-to-node encryption, [LuaSec](https://github.com/brunoos/luasec)

Once you have a local copy of this module's files under `src/`, add this to your Lua package path:

```
/path/to/src/?.lua;/path/to/src/?/init.lua;
```

## Documentation and examples

The current [documentation] targets version `0.3.6` only. `0.4.0` documentation should come soon.

[ngx_lua]: https://github.com/openresty/lua-nginx-module

[Luarocks]: https://luarocks.org
[lua-resty-cassandra]: https://github.com/jbochi/lua-resty-cassandra
[documentation]: http://thibaultcha.github.io/lua-cassandra/
[manual]: http://thibaultcha.github.io/lua-cassandra/manual/README.md.html

[badge-travis-url]: https://travis-ci.org/thibaultCha/lua-cassandra
[badge-travis-image]: https://img.shields.io/travis/thibaultCha/lua-cassandra.svg?style=flat

[badge-coveralls-url]: https://coveralls.io/r/thibaultCha/lua-cassandra?branch=master
[badge-coveralls-image]: https://coveralls.io/repos/thibaultCha/lua-cassandra/badge.svg?branch=master&style=flat

[badge-version-image]: https://img.shields.io/badge/version-0.3.6--0-blue.svg?style=flat
