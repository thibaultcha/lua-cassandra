# vim:set ts=4 sw=4 et fdm=marker syntax=:
use Test::Nginx::Socket::Lua;

our $HttpConfig = <<_EOC_;
    lua_package_path './lib/?.lua;./lib/?/init.lua;;';
_EOC_

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: cql raw types [byte]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local VALUES = { 1, 0, 3, 9 }
            local buf_w  = buffer.new_w()

            for i = 1, #VALUES do
                buffer.write_byte(buf_w, VALUES[i])
            end

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            for i = 1, #VALUES do
                local str = buffer.read_byte(buf_r)

                ngx.say(VALUES[i], " ", str)
            end
        }
    }
--- request
GET /t
--- response_body
1 1
0 0
3 3
9 9
--- no_error_log
[error]



=== TEST 2: cql raw types [int]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local VALUES = { 0, 12, 999, -1, 2^14, -2^14, -2147483647, 2147483647 }
            local buf_w  = buffer.new_w()

            for i = 1, #VALUES do
                buffer.write_int(buf_w, VALUES[i])
            end

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            for i = 1, #VALUES do
                local str = buffer.read_int(buf_r)

                ngx.say(VALUES[i], " ", str)
            end
        }
    }
--- request
GET /t
--- response_body
0 0
12 12
999 999
-1 -1
16384 16384
-16384 -16384
-2147483647 -2147483647
2147483647 2147483647
--- no_error_log
[error]



=== TEST 3: cql raw types [unset]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local buf_w = buffer.new_w()
            buffer.write_unset(buf_w)

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))
            local str = buffer.read_int(buf_r)

            ngx.say(str)
        }
    }
--- request
GET /t
--- response_body_like
-2
--- no_error_log
[error]



=== TEST 4: cql raw types [null]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local buf_w = buffer.new_w()
            buffer.write_null(buf_w)

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))
            local str = buffer.read_int(buf_r)

            ngx.say(str)
        }
    }
--- request
GET /t
--- response_body_like
-1
--- no_error_log
[error]



=== TEST 5: cql raw types [long]
--- SKIP
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local VALUES = { 0, 12, 999, -1, 2^14, -2^14, 9223372036854775807, -9223372036854775807 }
            local buf_w  = buffer.new_w()

            for i = 1, #VALUES do
                buffer.write_long(buf_w, VALUES[i])
            end

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            for i = 1, #VALUES do
                local str = buffer.read_long(buf_r)

                ngx.say(VALUES[i], " ", str)
            end
        }
    }
--- request
GET /t
--- response_body
0 0
12 12
999 999
-1 -1
16384 16384
-16384 -16384
9223372036854775807 9223372036854775807
-9223372036854775807 -9223372036854775807
--- no_error_log
[error]



=== TEST 6: cql raw types [short]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local VALUES = { 0, 128 }
            local buf_w  = buffer.new_w()

            for i = 1, #VALUES do
                buffer.write_long(buf_w, VALUES[i])
            end

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            for i = 1, #VALUES do
                local str = buffer.read_long(buf_r)

                ngx.say(VALUES[i], " ", str)
            end
        }
    }
--- request
GET /t
--- response_body
0 0
128 128
--- no_error_log
[error]



=== TEST 7: cql raw types [string]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local VALUES = {
                "hello world",
            }
            local buf_w = buffer.new_w()

            for i = 1, #VALUES do
                buffer.write_string(buf_w, VALUES[i])
            end

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            for i = 1, #VALUES do
                local str = buffer.read_string(buf_r)

                ngx.say(VALUES[i], " | ", str)
            end
        }
    }
--- request
GET /t
--- response_body
hello world | hello world
--- no_error_log
[error]



=== TEST 8: cql raw types [long string]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local VALUES = {
                string.rep("a", 2^20)
            }
            local buf_w = buffer.new_w()

            for i = 1, #VALUES do
                buffer.write_long_string(buf_w, VALUES[i])
            end

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            for i = 1, #VALUES do
                local str = buffer.read_long_string(buf_r)

                ngx.say(VALUES[i], " | ", str)
                ngx.say("same length: ", #VALUES[i] == #str)
            end
        }
    }
--- request
GET /t
--- response_body_like
a+ | a+
same length: true
--- no_error_log
[error]



=== TEST 9: cql raw types [bytes]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local VALUES = {
                "some bytes",
            }
            local buf_w = buffer.new_w()

            for i = 1, #VALUES do
                buffer.write_bytes(buf_w, VALUES[i])
            end

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            for i = 1, #VALUES do
                local str = buffer.read_bytes(buf_r)

                ngx.say(VALUES[i], " | ", str)
            end

            -- if n < 0 represents `null`
            buf_w = buffer.new_w()

            buffer.write_int(buf_w, -1)

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            local null = buffer.read_bytes(buf_r)

            ngx.say("n < 0 is cql.TYP_NULL: ", null == cql.TYP_NULL)
        }
    }
--- request
GET /t
--- response_body
some bytes | some bytes
n < 0 is cql.TYP_NULL: true
--- no_error_log
[error]



=== TEST 10: cql read_value()
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local buf_w = buffer.new_w()

            buffer.write_null(buf_w)
            buffer.write_unset(buf_w)
            buffer.write_value(buf_w, "hello world")

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            local value1 = buffer.read_value(buf_r)
            ngx.say("CQL NULL: ", value1 == cql.TYP_NULL)

            local value2 = buffer.read_value(buf_r)
            ngx.say("CQL UNSET: ", value2 == cql.TYP_UNSET)

            local value3 = buffer.read_value(buf_r)
            ngx.say(value3)
        }
    }
--- request
GET /t
--- response_body
CQL NULL: true
CQL UNSET: true
hello world
--- no_error_log
[error]



=== TEST 11: cql raw types [short_bytes]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local VALUES = {
                "hello world",
            }
            local buf_w  = buffer.new_w()

            for i = 1, #VALUES do
                buffer.write_short_bytes(buf_w, VALUES[i])
            end

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            for i = 1, #VALUES do
                local str = buffer.read_short_bytes(buf_r)

                ngx.say(VALUES[i], " | ", str)
            end
        }
    }
--- request
GET /t
--- response_body
hello world | hello world
--- no_error_log
[error]



=== TEST 12: cql raw types [uuid]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local VALUES = {
                "f5de06b0-57bb-11e7-9d3e-784f437104fa",
                "bb5f2954-5b6f-47de-ba7d-e26aed4d0c93",
            }
            local buf_w  = buffer.new_w()

            for i = 1, #VALUES do
                buffer.write_uuid(buf_w, VALUES[i])
            end

            -- read

            local buf_r = buffer.new_r(nil, buffer.get(buf_w))

            for i = 1, #VALUES do
                local str = buffer.read_uuid(buf_r)

                ngx.say(VALUES[i], " | ", str)
            end
        }
    }
--- request
GET /t
--- response_body
f5de06b0-57bb-11e7-9d3e-784f437104fa | f5de06b0-57bb-11e7-9d3e-784f437104fa
bb5f2954-5b6f-47de-ba7d-e26aed4d0c93 | bb5f2954-5b6f-47de-ba7d-e26aed4d0c93
--- no_error_log
[error]



=== TEST 13: cql raw types [inet]
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql    = require "cassandra.cql2"
            local buffer = cql.buffer

            -- write

            local VALUES = {
                "127.0.0.1",
                "127.0.0.1:8080",
                "2001:0db8:85a3:0042:1000:8a2e:0370:7334",
                "2001:0db8:0000:0000:0000:0000:0000:0001",
                "::1",
                "[2001:0db8:85a3:0042:1000:8a2e:0370:7334]:8080",
                "[::1]:8080",
            }

            for i = 1, #VALUES do
                local buf_w  = buffer.new_w()
                buffer.write_inet(buf_w, VALUES[i])

                -- read
                -- inet has unpredictable size...

                local buf_r = buffer.new_r(nil, buffer.get(buf_w))
                local str = buffer.read_inet(buf_r)

                ngx.say(VALUES[i], " | ", str)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.1 | 127.0.0.1
127.0.0.1:8080 | 127.0.0.1:8080
2001:0db8:85a3:0042:1000:8a2e:0370:7334 | 2001:0db8:85a3:0042:1000:8a2e:0370:7334
2001:0db8:0000:0000:0000:0000:0000:0001 | 2001:0db8:0000:0000:0000:0000:0000:0001
::1 | 0000:0000:0000:0000:0000:0000:0000:0001
[2001:0db8:85a3:0042:1000:8a2e:0370:7334]:8080 | [2001:0db8:85a3:0042:1000:8a2e:0370:7334]:8080
[::1]:8080 | [0000:0000:0000:0000:0000:0000:0000:0001]:8080
--- no_error_log
[error]



