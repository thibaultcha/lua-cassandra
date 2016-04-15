# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;

our $HttpConfig = <<_EOC_;
    lua_package_path 'lib/?.lua;lib/?/init.lua;;';
_EOC_

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: rr_lb sanity
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local dc_rr = require 'resty.cassandra.policies.lb.dc_rr'
            ngx.say(dc_rr.name)

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2'},

                {host = '127.0.0.1', data_center = 'dc1'},
                {host = '127.0.0.2', data_center = 'dc1'},
                {host = '127.0.0.3', data_center = 'dc1'},

                {host = '10.0.0.2', data_center = 'dc2'},
                {host = '10.0.0.3', data_center = 'dc2'}
            }

            local lb = dc_rr.new('dc1')
            ngx.say('local_dc: ', lb.local_dc)

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
dc_aware_round_robin
local_dc: dc1

1 127.0.0.1
2 127.0.0.2
3 127.0.0.3
4 10.0.0.1
5 10.0.0.2
6 10.0.0.3

1 127.0.0.2
2 127.0.0.3
3 127.0.0.1
4 10.0.0.2
5 10.0.0.3
6 10.0.0.1

1 127.0.0.3
2 127.0.0.1
3 127.0.0.2
4 10.0.0.3
5 10.0.0.1
6 10.0.0.2
--- no_error_log
[error]



=== TEST 2: rr_lb on loop break
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local dc_rr = require 'resty.cassandra.policies.lb.dc_rr'
            ngx.say(dc_rr.name)

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2'},

                {host = '127.0.0.1', data_center = 'dc1'},
                {host = '127.0.0.2', data_center = 'dc1'},
                {host = '127.0.0.3', data_center = 'dc1'},

                {host = '10.0.0.2', data_center = 'dc2'},
                {host = '10.0.0.3', data_center = 'dc2'}
            }

            local lb = dc_rr.new('dc1')
            ngx.say('local_dc: ', lb.local_dc)

            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say(i, ' ', peer.host)
                ngx.log(ngx.INFO, i, ' ', peer.host)
                if i == #peers - 1 then
                    break
                end
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say(i, ' ', peer.host)
                ngx.log(ngx.INFO, i, ' ', peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
dc_aware_round_robin
local_dc: dc1

1 127.0.0.1
2 127.0.0.2
3 127.0.0.3
4 10.0.0.1
5 10.0.0.2

1 127.0.0.2
2 127.0.0.3
3 127.0.0.1
4 10.0.0.2
5 10.0.0.3
6 10.0.0.1
--- no_error_log
[error]
