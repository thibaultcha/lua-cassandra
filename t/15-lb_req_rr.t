# vim:set ts=4 sw=4 et fdm=marker:
use lib '.';
use Test::Nginx::Socket::Lua;
use t::Util;

no_long_string();

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: lb_req_rr sanity
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_req_rr = require 'resty.cassandra.policies.lb.req_rr'
            ngx.say(lb_req_rr.name)

            local peers = {
                {host = '127.0.0.1'},
                {host = '127.0.0.2'},
                {host = '127.0.0.3'}
            }

            local lb = lb_req_rr.new()
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
req_round_robin

1 127.0.0.1
2 127.0.0.2
3 127.0.0.3

1 127.0.0.3
2 127.0.0.2
3 127.0.0.1

1 127.0.0.1
2 127.0.0.3
3 127.0.0.2
--- no_error_log
[error]



=== TEST 2: lb_req_rr on loop break
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_req_rr = require 'resty.cassandra.policies.lb.req_rr'
            ngx.say(lb_req_rr.name)

            local peers = {
                {host = '127.0.0.1'},
                {host = '127.0.0.2'},
                {host = '127.0.0.3'}
            }

            local lb = lb_req_rr.new()
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
req_round_robin

1 127.0.0.1
2 127.0.0.2

1 127.0.0.2
2 127.0.0.3
3 127.0.0.1
--- no_error_log
[error]



=== TEST 3: lb_req_rr returns same host first when invoked multiple times
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local lb_req_rr = require 'resty.cassandra.policies.lb.req_rr'
            ngx.say(lb_req_rr.name)

            local peers = {
                {host = '127.0.0.1'},
                {host = '127.0.0.2'},
                {host = '127.0.0.3'}
            }

            local lb = lb_req_rr.new()
            lb:init(peers)

            for i, peer in lb:iter() do
                ngx.say('1. ', peer.host)
                if i == 1 then
                    break
                end
            end

            for i, peer in lb:iter() do
                ngx.say('2. ', peer.host)
                if i == 1 then
                    break
                end
            end

            for i, peer in lb:iter() do
                ngx.say('3. ', peer.host)
                if i == 1 then
                    break
                end
            end
        }
    }
--- request
GET /t
--- response_body
req_round_robin
1. 127.0.0.1
2. 127.0.0.1
3. 127.0.0.1
--- no_error_log
[error]



=== TEST 4: lb_req_dc_rr is resilient when ngx.ctx is 'nil'
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            if rawget(ngx, "ctx") == nil then
                -- OpenResty >= 1.15.8.1
                local __ngx_index = getmetatable(ngx)

                setmetatable(ngx, {
                    __index = function(t, k)
                        if k == "ctx" then
                            return
                        end

                        return __ngx_index(t, k)
                    end
                })

            else
                -- OpenResty <= 1.13.6.2
                ngx.ctx = nil
            end

            local lb_req_rr = require 'resty.cassandra.policies.lb.req_rr'
            ngx.say(lb_req_rr.name)

            local peers = {
                {host = '127.0.0.1'},
                {host = '127.0.0.2'},
                {host = '127.0.0.3'}
            }

            local lb = lb_req_rr.new()
            lb:init(peers)

            for i, peer in lb:iter() do
                ngx.say('1. ', peer.host)
                if i == 1 then
                    break
                end
            end

            for i, peer in lb:iter() do
                ngx.say('2. ', peer.host)
                if i == 1 then
                    break
                end
            end

            for i, peer in lb:iter() do
                ngx.say('3. ', peer.host)
                if i == 1 then
                    break
                end
            end
        }
    }
--- request
GET /t
--- response_body
req_round_robin
1. 127.0.0.1
2. 127.0.0.2
3. 127.0.0.3
--- no_error_log
[error]



=== TEST 5: req_rr can be used in init_by_lua* context with resty.core
--- http_config eval
qq{
    $::LuaPackagePath

    init_by_lua_block {
        require "resty.core"

        _G.res = {}

        local lb_req_rr = require 'resty.cassandra.policies.lb.req_rr'
        table.insert(res, lb_req_rr.name)

        local peers = {
            {host = '127.0.0.1'},
            {host = '127.0.0.2'},
            {host = '127.0.0.3'}
        }

        local lb = lb_req_rr.new()
        lb:init(peers)

        for i, peer in lb:iter() do
            table.insert(res, '1. ' .. peer.host)
            if i == 1 then
                break
            end
        end

        for i, peer in lb:iter() do
            table.insert(res, '2. ' .. peer.host)
            if i == 1 then
                break
            end
        end

        for i, peer in lb:iter() do
            table.insert(res, '3. ' .. peer.host)
            if i == 1 then
                break
            end
        end
    }
}
--- config
    location /t {
        content_by_lua_block {
            ngx.say(table.concat(res, "\n"))
        }
    }
--- request
GET /t
--- response_body
req_round_robin
1. 127.0.0.1
2. 127.0.0.2
3. 127.0.0.3
--- no_error_log
[error]
