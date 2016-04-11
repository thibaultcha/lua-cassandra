# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;

our $HttpConfig = <<_EOC_;
    lua_package_path 'lib/?.lua;lib/?/init.lua;;';
    lua_shared_dict cassandra 1m;
_EOC_

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: cluster.new() invalid opts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new('')
            if not cluster then
                ngx.say(err)
            end

            cluster, err = Cluster.new({shm = true})
            if not cluster then
                ngx.say(err)
            end

            cluster, err = Cluster.new({shm = 'invalid_shm'})
            if not cluster then
                ngx.say(err)
            end
        }
    }
--- request
GET /t
--- response_body
opts must be a table
shm must be a string
no shared dict invalid_shm
--- no_error_log
[error]



=== TEST 2: cluster.init() default opts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
            end
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 3: cluster.refresh() with invalid contact_points
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                contact_points = {'127.0.0.255'},
                connect_timeout = 10
            }
            if not cluster then
                ngx.log(ngx.ERR, err)
            end

            local ok, err = cluster:refresh()
            ngx.say(ok)
            ngx.say(err)
        }
    }
--- request
GET /t
--- response_body
nil
all hosts tried for query failed. 127.0.0.255: timeout
--- error_log eval
qr/\[error\] .*? connect timed out/



=== TEST 4: cluster.refresh() sets hosts in shm
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, 'could not spawn cluster: ', err)
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, 'could not refresh: ', err)
            end

            local peers, err = cluster:get_shm_peers()
            if not peers then
                ngx.log(ngx.ERR, 'could not get shm peers: ', err)
            end

            for i = 1, #peers do
                local p = peers[i]
                ngx.say(p.host..' '..p.unhealthy_at..' '..p.reconn_delay)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.3 0 0
127.0.0.2 0 0
127.0.0.1 0 0
--- no_error_log
[error]



=== TEST 5: cluster.refresh() removes old peers
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
            end

            -- insert fake peers
            cluster:set_shm_peer('127.0.0.253', {
                reconn_delay = 0,
                unhealthy_at = 0,
                port = 9042
            })
            cluster:set_shm_peer('127.0.0.254', {
                reconn_delay = 0,
                unhealthy_at = 0,
                port = 9042
            })

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
            end

            local peers, err = cluster:get_shm_peers()
            if not peers then
                ngx.log(ngx.ERR, err)
            end

            for i = 1, #peers do
                ngx.say(peers[i].host)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.3
127.0.0.2
127.0.0.1
--- no_error_log
[error]



=== TEST 6: get_peers() corrupted shm
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
            end

            cluster.shm:set('127.0.0.1', 'foobar')

            local peers, err = cluster:get_shm_peers()
            if not peers then
                ngx.log(ngx.ERR, err)
            end
        }
    }
--- request
GET /t
--- response_body

--- error_log
corrupted shm

