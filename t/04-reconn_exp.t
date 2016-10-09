# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;
use t::Util;

our $HttpConfig = $t::Util::HttpConfig;

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: reconn_exp next_delay() sanity
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local reconn_exp = require 'resty.cassandra.policies.reconnection.exp'
            ngx.say(reconn_exp.name)

            local reconn = reconn_exp.new(1000, 60000) -- 1s, 60s

            for i = 1, 10 do
                ngx.say(reconn:next_delay('127.0.0.1'))
            end
        }
    }
--- request
GET /t
--- response_body
exponential
1000
4000
9000
16000
25000
36000
49000
60000
60000
60000
--- no_error_log
[error]



=== TEST 2: reconn_exp next_delay() distinct hosts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local reconn_exp = require 'resty.cassandra.policies.reconnection.exp'
            local reconn = reconn_exp.new(1000, 60000) -- 1s, 60s

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
.1 4000
.1 9000
.2 1000
.2 4000
.1 16000
.1 25000
.2 9000
.1 36000
--- no_error_log
[error]



=== TEST 3: reconn_exp next_delay() base and max
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local reconn_exp = require 'resty.cassandra.policies.reconnection.exp'
            local reconn = reconn_exp.new(4000, 20000)

            for i = 1, 5 do
                ngx.say(reconn:next_delay('127.0.0.1'))
            end
        }
    }
--- request
GET /t
--- response_body
4000
16000
20000
20000
20000
--- no_error_log
[error]



=== TEST 4: reconn_exp input check
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local reconn_exp = require 'resty.cassandra.policies.reconnection.exp'

            local ok, err = pcall(reconn_exp.new)
            ngx.say(ok, ' ', err)

            ok, err = pcall(reconn_exp.new, 0)
            ngx.say(ok, ' ', err)

            ok, err = pcall(reconn_exp.new, 1)
            ngx.say(ok, ' ', err)

            ok, err = pcall(reconn_exp.new, 1, 0)
            ngx.say(ok, ' ', err)
        }
    }
--- request
GET /t
--- response_body
false arg #1 base_delay must be a positive integer
false arg #1 base_delay must be a positive integer
false arg #2 max_delay must be a positive integer
false arg #2 max_delay must be a positive integer
--- no_error_log
[error]



=== TEST 5: reconn_exp reset()
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local reconn_exp = require 'resty.cassandra.policies.reconnection.exp'

            local reconn = reconn_exp.new(1000, 60000)
            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))
            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))
            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))

            ngx.say('.2 ', reconn:next_delay('127.0.0.2'))
            ngx.say('.2 ', reconn:next_delay('127.0.0.2'))
            ngx.say('.2 ', reconn:next_delay('127.0.0.2'))

            reconn:reset('127.0.0.1')

            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))
            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))
            ngx.say('.1 ', reconn:next_delay('127.0.0.1'))

            ngx.say('.2 ', reconn:next_delay('127.0.0.2'))
        }
    }
--- request
GET /t
--- response_body
.1 1000
.1 4000
.1 9000
.2 1000
.2 4000
.2 9000
.1 1000
.1 4000
.1 9000
.2 16000
--- no_error_log
[error]



=== TEST 6: reconn_exp reset() ignores invalid hosts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local reconn_exp = require 'resty.cassandra.policies.reconnection.exp'

            local reconn = reconn_exp.new(1000, 60000)
            reconn:reset('foobar')
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]
