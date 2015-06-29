# lua-cassandra

Pure Lua Cassandra client using CQL binary protocol v2/v3.

It is 100% non-blocking if used in Nginx/Openresty but can also be used with luasocket.

This project is a fork of [jbochi#lua-resty-cassandra][lua-resty-cassandra]. It adds support for binary protocol v3, a few bug fixes and more to come.

[lua-resty-cassandra]: https://github.com/jbochi/lua-resty-cassandra