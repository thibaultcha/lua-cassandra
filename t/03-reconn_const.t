# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;
use t::Util;

our $HttpConfig = $t::Util::HttpConfig;

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: reconn_const next_delay() sanity
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local reconn_cst = require 'resty.cassandra.policies.reconnection.const'
            ngx.say(reconn_cst.name)

            local reconn = reconn_cst.new(1000) -- 1s

            for i = 1, 5 do
                ngx.say(reconn:next_delay('127.0.0.1'))
            end
        }
    }
--- request
GET /t
--- response_body
constant
1000
1000
1000
1000
1000
--- no_error_log
[error]



=== TEST 2: reconn_const next_delay() distinct hosts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local reconn_cst = require 'resty.cassandra.policies.reconnection.const'
            local reconn = reconn_cst.new(1000)

            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))
            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))
            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))
            ngx.say('.2 ', reconn:next_delay('127.0.0.2'))
            ngx.say('.2 ', reconn:next_delay('127.0.0.2'))
            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))
            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))
            ngx.say('.2 ', reconn:next_delay('127.0.0.2'))
            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))

        }
    }
--- request
GET /t
--- response_body
.1 1000
.1 1000
.1 1000
.2 1000
.2 1000
.1 1000
.1 1000
.2 1000
.1 1000
--- no_error_log
[error]



=== TEST 3: reconn_const input check
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local reconn_cst = require 'resty.cassandra.policies.reconnection.const'
            local ok, err = pcall(reconn_cst.new)
            ngx.say(ok, ' ', err)

            ok, err = pcall(reconn_cst.new, 0)
            ngx.say(ok, ' ', err)
        }
    }
--- request
GET /t
--- response_body
false arg #1 delay must be a positive integer
false arg #1 delay must be a positive integer
--- no_error_log
[error]



=== TEST 4: reconn_const reset()
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local reconn_cst = require 'resty.cassandra.policies.reconnection.const'

            local reconn = reconn_cst.new(1000) -- 1s

            for i = 1, 5 do
                ngx.say(reconn:next_delay('127.0.0.1'))
            end

            reconn:reset()

            for i = 1, 5 do
                ngx.say(reconn:next_delay('127.0.0.1'))
            end
        }
    }
--- request
GET /t
--- response_body
1000
1000
1000
1000
1000
1000
1000
1000
1000
1000
--- no_error_log
[error]
