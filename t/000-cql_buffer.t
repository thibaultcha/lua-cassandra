# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;

our $HttpConfig = <<_EOC_;
    lua_package_path './lib/?.lua;./lib/?/init.lua;;';
_EOC_

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: cql buffer write() must write string
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql = require "cassandra.cql2"
            local buffer = cql.buffer

            local buf_w = buffer.new_w()
            assert(type(buf_w) == "table")

            local ok, err = pcall(buffer.write, buf_w, false)
            if not ok then
                ngx.say(err)
            end
        }
    }
--- request
GET /t
--- response_body
bytes must be a string
--- no_error_log
[error]



=== TEST 2: cql buffer write() writes to buffer
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql = require "cassandra.cql2"
            local buffer = cql.buffer

            local buf_w = buffer.new_w()
            assert(type(buf_w) == "table")

            buffer.write(buf_w, "hello")
            buffer.write(buf_w, "world")

            ngx.say("buf size: ", buf_w.i, " buf.t size: ", #buf_w.t)
        }
    }
--- request
GET /t
--- response_body
buf size: 3 buf.t size: 2
--- no_error_log
[error]



=== TEST 3: cql buffer read() must read a positive amount of bytes
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql = require "cassandra.cql2"
            local buffer = cql.buffer

            local buf_r = buffer.new_r(nil, "some bytes")
            assert(type(buf_r) == "table")

            local ok, err = pcall(buffer.read, buf_r, 0)
            if not ok then
                ngx.say(err)
            end
        }
    }
--- request
GET /t
--- response_body
must read a positive number of bytes
--- no_error_log
[error]



=== TEST 4: cql buffer read() reads the given number of bytes
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql = require "cassandra.cql2"
            local buffer = cql.buffer

            local buf_r = buffer.new_r(nil, "hello world")
            assert(type(buf_r) == "table")

            local str = buffer.read(buf_r, 5)
            ngx.say(str)

            local str2 = buffer.read(buf_r, 3)
            ngx.say(str2)

            local str3 = buffer.read(buf_r, 3)
            ngx.say(str3)
        }
    }
--- request
GET /t
--- response_body
hello
 wo
rld
--- no_error_log
[error]



=== TEST 5: cql buffer read() reads all bytes at once if no n_bytes
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql = require "cassandra.cql2"
            local buffer = cql.buffer

            local buf_r = buffer.new_r(nil, "hello world")
            assert(type(buf_r) == "table")

            local str = buffer.read(buf_r)
            ngx.say(str)
        }
    }
--- request
GET /t
--- response_body
hello world
--- no_error_log
[error]



=== TEST 6: cql buffer read() returns empty string when buffer is read
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql = require "cassandra.cql2"
            local buffer = cql.buffer

            local buf_r = buffer.new_r(nil, "hello world")
            assert(type(buf_r) == "table")

            local str = buffer.read(buf_r)
            ngx.say(str)

            local str2 = buffer.read(buf_r)
            ngx.say(str2 == "")
        }
    }
--- request
GET /t
--- response_body
hello world
true
--- no_error_log
[error]



=== TEST 7: cql buffer get() concats a write buffer
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql = require "cassandra.cql2"
            local buffer = cql.buffer

            local buf_w = buffer.new_w()
            assert(type(buf_w) == "table")

            buffer.write(buf_w, "hello")
            buffer.write(buf_w, "world")
            buffer.write(buf_w, "hello")
            buffer.write(buf_w, "world")
            buffer.write(buf_w, "end")

            ngx.say(buffer.get(buf_w))
        }
    }
--- request
GET /t
--- response_body
helloworldhelloworldend
--- no_error_log
[error]



=== TEST 8: cql buffer copy() copies a write buffer
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql = require "cassandra.cql2"
            local buffer = cql.buffer

            local buf_w = buffer.new_w()
            assert(type(buf_w) == "table")

            local buf_w2 = buffer.new_w()

            buffer.write(buf_w, "hello")
            buffer.write(buf_w, "world")
            buffer.write(buf_w, "end")

            buffer.copy(buf_w, buf_w2)

            ngx.say(buffer.get(buf_w2))
        }
    }
--- request
GET /t
--- response_body
helloworldend
--- no_error_log
[error]
