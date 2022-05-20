# vim:set ts=4 sw=4 et fdm=marker:
use lib '.';
use Test::Nginx::Socket::Lua;
use t::Util;

no_long_string();

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: lb_req_dc_rr sanity
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local req_dc_rr = require 'resty.cassandra.policies.lb.req_dc_rr'
            ngx.say(req_dc_rr.name)

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2'},

                {host = '127.0.0.1', data_center = 'dc1'},
                {host = '127.0.0.2', data_center = 'dc1'},
                {host = '127.0.0.3', data_center = 'dc1'},

                {host = '10.0.0.2', data_center = 'dc2'},
                {host = '10.0.0.3', data_center = 'dc2'}
            }

            local lb = req_dc_rr.new('dc1')
            ngx.say('local_dc: ', lb.local_dc)

            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("1. ", peer.host)
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("2. ", peer.host)
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("3. ", peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
req_and_dc_aware_round_robin
local_dc: dc1

1. 127.0.0.1
1. 127.0.0.2
1. 127.0.0.3
1. 10.0.0.1
1. 10.0.0.2
1. 10.0.0.3

2. 127.0.0.3
2. 127.0.0.2
2. 127.0.0.1
2. 10.0.0.2
2. 10.0.0.3
2. 10.0.0.1

3. 127.0.0.1
3. 127.0.0.3
3. 127.0.0.2
3. 10.0.0.3
3. 10.0.0.1
3. 10.0.0.2
--- no_error_log
[error]



=== TEST 2: lb_req_dc_rr on loop break
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local req_dc_rr = require 'resty.cassandra.policies.lb.req_dc_rr'
            ngx.say(req_dc_rr.name)

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2'},

                {host = '127.0.0.1', data_center = 'dc1'},
                {host = '127.0.0.2', data_center = 'dc1'},
                {host = '127.0.0.3', data_center = 'dc1'},

                {host = '10.0.0.2', data_center = 'dc2'},
                {host = '10.0.0.3', data_center = 'dc2'}
            }

            local lb = req_dc_rr.new('dc1')
            ngx.say('local_dc: ', lb.local_dc)

            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("1. ", peer.host)
                if i == #peers - 1 then
                    break
                end
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("2. ", peer.host)
                ngx.log(ngx.INFO, i, ' ', peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
req_and_dc_aware_round_robin
local_dc: dc1

1. 127.0.0.1
1. 127.0.0.2
1. 127.0.0.3
1. 10.0.0.1
1. 10.0.0.2
1. 10.0.0.3

2. 127.0.0.3
2. 127.0.0.2
2. 127.0.0.1
2. 10.0.0.2
2. 10.0.0.3
2. 10.0.0.1
--- no_error_log
[error]



=== TEST 3: lb_req_dc_rr with missing local_dc
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local req_dc_rr = require 'resty.cassandra.policies.lb.req_dc_rr'
            local lb = req_dc_rr.new()
        }
    }
--- request
GET /t
--- error_code: 500
--- error_log
local_dc must be a string
--- no_error_log
[crit]



=== TEST 4: lb_req_dc_rr with missing data_center fields
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local req_dc_rr = require 'resty.cassandra.policies.lb.req_dc_rr'

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2'},

                {host = '127.0.0.1', data_center = 'dc1'},
                {host = '127.0.0.2', data_center = 'dc1'},
                {host = '127.0.0.3'}
            }

            local lb = req_dc_rr.new('dc1')
            ngx.say("local_dc: ", lb.local_dc)

            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("1. ", peer.host)
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("2. ", peer.host)
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("3. ", peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
local_dc: dc1

1. 127.0.0.1
1. 127.0.0.2
1. 10.0.0.1
1. 127.0.0.3

2. 127.0.0.2
2. 127.0.0.1
2. 127.0.0.3
2. 10.0.0.1

3. 127.0.0.1
3. 127.0.0.2
3. 10.0.0.1
3. 127.0.0.3
--- error_log eval
qr/\[warn\].*?\[lua-cassandra\] peer 127\.0\.0\.3 has no data_center field in shm, considering it remote/



=== TEST 5: lb_req_dc_rr with hyphens in dc name
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local req_dc_rr = require "resty.cassandra.policies.lb.req_dc_rr"

            local peers = {
                { host = "10.0.0.1", data_center = "europe-west1-b" },

                { host = "127.0.0.1", data_center = "dc1"},
                { host = "127.0.0.2", data_center = "dc1"},
                { host = "127.0.0.3", data_center = "dc1"},
            }

            local lb = req_dc_rr.new("europe-west1-b")
            ngx.say("local_dc: ", lb.local_dc)

            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("1. ", peer.host)
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("2. ", peer.host)
            end

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("3. ", peer.host)
            end
        }
    }
--- request
GET /t
--- response_body
local_dc: europe-west1-b

1. 10.0.0.1
1. 127.0.0.1
1. 127.0.0.2
1. 127.0.0.3

2. 10.0.0.1
2. 127.0.0.2
2. 127.0.0.3
2. 127.0.0.1

3. 10.0.0.1
3. 127.0.0.3
3. 127.0.0.1
3. 127.0.0.2
--- no_error_log
[error]



=== TEST 6: lb_req_dc_rr returns same host first when invoked multiple times
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local req_dc_rr = require 'resty.cassandra.policies.lb.req_dc_rr'
            ngx.say(req_dc_rr.name)

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2'},

                {host = '127.0.0.1', data_center = 'dc1'},
                {host = '127.0.0.2', data_center = 'dc1'},
                {host = '127.0.0.3', data_center = 'dc1'},

                {host = '10.0.0.2', data_center = 'dc2'},
                {host = '10.0.0.3', data_center = 'dc2'}
            }

            local lb = req_dc_rr.new('dc1')
            ngx.say('local_dc: ', lb.local_dc)

            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("1. ", peer.host)
                break
            end

            for i, peer in lb:iter() do
                ngx.say("2. ", peer.host)
                break
            end

            for i, peer in lb:iter() do
                ngx.say("3. ", peer.host)
                break
            end
        }
    }
--- request
GET /t
--- response_body
req_and_dc_aware_round_robin
local_dc: dc1

1. 127.0.0.1
2. 127.0.0.1
3. 127.0.0.1
--- no_error_log
[error]



=== TEST 7: lb_req_dc_rr is resilient when ngx.ctx is 'nil'
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

            local req_dc_rr = require 'resty.cassandra.policies.lb.req_dc_rr'
            ngx.say(req_dc_rr.name)

            local peers = {
                {host = '10.0.0.1', data_center = 'dc2'},

                {host = '127.0.0.1', data_center = 'dc1'},
                {host = '127.0.0.2', data_center = 'dc1'},
                {host = '127.0.0.3', data_center = 'dc1'},

                {host = '10.0.0.2', data_center = 'dc2'},
                {host = '10.0.0.3', data_center = 'dc2'}
            }

            local lb = req_dc_rr.new('dc1')
            ngx.say('local_dc: ', lb.local_dc)

            lb:init(peers)

            ngx.say()
            for i, peer in lb:iter() do
                ngx.say("1. ", peer.host)
                break
            end

            for i, peer in lb:iter() do
                ngx.say("2. ", peer.host)
                break
            end

            for i, peer in lb:iter() do
                ngx.say("3. ", peer.host)
                break
            end
        }
    }
--- request
GET /t
--- response_body
req_and_dc_aware_round_robin
local_dc: dc1

1. 127.0.0.1
2. 127.0.0.2
3. 127.0.0.3
--- no_error_log
[error]



=== TEST 8: lb_req_dc_rr can be used in init_by_lua* context with resty.core
--- http_config eval
qq{
    $::LuaPackagePath

    init_by_lua_block {
        require "resty.core"

        _G.res = {}

        local req_dc_rr = require 'resty.cassandra.policies.lb.req_dc_rr'

        table.insert(res, req_dc_rr.name)

        local peers = {
            {host = '10.0.0.1', data_center = 'dc2'},

            {host = '127.0.0.1', data_center = 'dc1'},
            {host = '127.0.0.2', data_center = 'dc1'},
            {host = '127.0.0.3', data_center = 'dc1'},

            {host = '10.0.0.2', data_center = 'dc2'},
            {host = '10.0.0.3', data_center = 'dc2'}
        }

        local lb = req_dc_rr.new('dc1')
        table.insert(res, 'local_dc: ' .. lb.local_dc)

        lb:init(peers)

        table.insert(res, '')
        for i, peer in lb:iter() do
            table.insert(res, "1. " .. peer.host)
            break
        end

        for i, peer in lb:iter() do
            table.insert(res, "2. " .. peer.host)
            break
        end

        for i, peer in lb:iter() do
            table.insert(res, "3. " .. peer.host)
            break
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
req_and_dc_aware_round_robin
local_dc: dc1

1. 127.0.0.1
2. 127.0.0.2
3. 127.0.0.3
--- no_error_log
[error]
