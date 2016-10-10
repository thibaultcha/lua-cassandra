# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;
use t::Util;

our $HttpConfig = $t::Util::HttpConfig;

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: retry_simple on_read_timeout() sanity
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local retry_simple = require 'resty.cassandra.policies.retry.simple'
            ngx.say(retry_simple.name)

            local retry = retry_simple.new(3)

            for i = 0, 4 do
                ngx.say(i, ' ', retry:on_read_timeout({retries = i}))
            end
        }
    }
--- request
GET /t
--- response_body
simple
0 true
1 true
2 true
3 false
4 false
--- no_error_log
[error]



=== TEST 2: retry_simple on_write_timeout() sanity
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local retry_simple = require 'resty.cassandra.policies.retry.simple'
            local retry = retry_simple.new(3)

            for i = 0, 4 do
                ngx.say(i, ' ', retry:on_write_timeout({retries = i}))
            end
        }
    }
--- request
GET /t
--- response_body
0 true
1 true
2 true
3 false
4 false
--- no_error_log
[error]



=== TEST 3: retry_simple on_unavailable() sanity
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local retry_simple = require 'resty.cassandra.policies.retry.simple'
            local retry = retry_simple.new(3)

            for i = 0, 4 do
                ngx.say(i, ' ', retry:on_unavailable({retries = i}))
            end
        }
    }
--- request
GET /t
--- response_body
0 false
1 false
2 false
3 false
4 false
--- no_error_log
[error]



=== TEST 4: retry_simple input check
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local retry_simple = require 'resty.cassandra.policies.retry.simple'

            local ok, err = pcall(retry_simple.new)
            ngx.say(ok, ' ', err)

            ok, err = pcall(retry_simple.new, 0)
            ngx.say(ok, ' ', err)
        }
    }
--- request
GET /t
--- response_body
false arg #1 max_retries must be a positive integer
false arg #1 max_retries must be a positive integer
--- no_error_log
[error]
