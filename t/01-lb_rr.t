# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;
use t::Util;

our $HttpConfig = $t::Util::HttpConfig;

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: rr_lb sanity
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_rr = require 'resty.cassandra.policies.lb.rr'
            ngx.say(lb_rr.name)

            local peers = {
                {host = '127.0.0.1'},
                {host = '127.0.0.2'},
                {host = '127.0.0.3'}
            }

            local lb = lb_rr.new()
            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say(i, ' ', peer.host)
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say(i, ' ', peer.host)
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say(i, ' ', peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
round_robin

1 127.0.0.1
2 127.0.0.2
3 127.0.0.3

1 127.0.0.2
2 127.0.0.3
3 127.0.0.1

1 127.0.0.3
2 127.0.0.1
3 127.0.0.2
--- no_error_log
[error]



=== TEST 2: rr_lb on loop break
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_rr = require 'resty.cassandra.policies.lb.rr'
            ngx.say(lb_rr.name)

            local peers = {
                {host = '127.0.0.1'},
                {host = '127.0.0.2'},
                {host = '127.0.0.3'}
            }

            local lb = lb_rr.new()
            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say(i, ' ', peer.host)
                if i == #peers - 1 then
                    break
                end
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say(i, ' ', peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
round_robin

1 127.0.0.1
2 127.0.0.2

1 127.0.0.2
2 127.0.0.3
3 127.0.0.1
--- no_error_log
[error]
