# lua-cassandra

A pure Lua client library for Apache Cassandra (2.x), compatible with
[OpenResty][OpenResty].

This library offers 2 modules: a "single host" module (`cassandra`), compatible
with PUC Lua 5.1/5.2, LuaJIT and OpenResty, which allows your application to
connect itself to a given Cassandra node, and a "cluster" module
(`resty.cassandra.cluster`), only compatible with OpenResty which adds support
for multi-node Cassandra datacenters.

The following table describes which version(s) of the binary protocol is
supported by each Cassandra version:

<br />
<table class="module_list">
  <tr><td>Cassandra Version</td><td>Binary Protocol support</td></tr>
  <tr><td>1.2</td><td>1</td></tr>
  <tr><td>2.0</td><td>1, 2</td></tr>
  <tr><td>2.1</td><td>1, 2, 3</td></tr>
  <tr><td>2.2</td><td>1, 2, 3, 4</td></tr>
  <tr><td>3.0</td><td>1, 2, 3, 4</td></tr>
</table>
<br />

This library supports binary protocols 2 and 3, hence supports Cassandra 2.x
only (3.0 currently has some incompatibilities).

## Installation

With [Luarocks][Luarocks]:

```bash
$ luarocks install lua-cassandra
```

Manually:

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

See the `cassandra` and `resty.cassandra.cluster` modules references for a
detailed list of available methods and options.

## Examples

Also check out the examples section for concrete examples of basic and advanced
usage.

## Credits

This project was originally a fork of
[jbochi/lua-resty-cassandra][lua-resty-cassandra] with bugfixes and new
features. It was completely rewritten in its `0.4.0` version to allow serious
improvements in terms of features and maintainability.

## License

The MIT License (MIT)

Original work Copyright (c) 2016 Thibault Charbonnier
Based on the work of Juarez Bochi Copyright 2014

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

[OpenResty]: https://openresty.org
[Luarocks]: https://luarocks.org
[lua-resty-cassandra]: https://github.com/jbochi/lua-resty-cassandra
