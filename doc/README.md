# lua-cassandra

This library is a pure Lua implementation of the Cassandra CQL binary protocol.

It is compatible in Lua 5.1, 5.2, 5.3, LuaJIT, and optimized for [OpenResty][OpenResty]/[ngx_lua][ngx_lua].

The following table describes which version(s) of the binary protocol is supported by each Cassandra version:

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

This library supports binary protocols 2 and 3, hence supports Cassandra 2.0+. It is tested with Cassandra 2.1 and 2.2 only as of now, with plans for testing it with more versions.

## Installation

With [Luarocks][Luarocks]:

```bash
$ luarocks install lua-cassandra
```

Manually:

Once you have a local copy of this module's `src/` directory, add it to your `LUA_PATH` (or `lua_package_path` directive for ngx_lua):

```
/path/to/src/?.lua;/path/to/src/?/init.lua;
```

**Note**: If used *outside* of ngx_lua, this module requires:

- [LuaSocket](http://w3.impa.br/~diego/software/luasocket/)
- If you wish to use TLS client-to-node encryption, [LuaSec](https://github.com/brunoos/luasec)

## Usage

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

See the `cassandra` module for a detailed list of available objects and functions.

## Examples

Also check out the examples section for concrete examples of basic and advanced usage.

## Credits

This project was originally a fork of [jbochi/lua-resty-cassandra][lua-resty-cassandra] with bugfixes and new features. It was completely rewritten in its `0.4.0` version to allow serious improvements in terms of features and maintainability.

[OpenResty]: https://openresty.org
[ngx_lua]: https://github.com/openresty/lua-nginx-module
[Luarocks]: https://luarocks.org
[lua-resty-cassandra]: https://github.com/jbochi/lua-resty-cassandra
