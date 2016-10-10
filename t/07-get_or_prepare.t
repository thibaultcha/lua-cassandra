# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;
use t::Util;

our $HttpConfig = $t::Util::HttpConfig;

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: cluster.get_or_prepare() returns query_id
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, 'could not refresh: ', err)
                return
            end

            local coordinator, err = cluster:next_coordinator()
            if not coordinator then
                ngx.log(ngx.ERR, 'could not get coordinator: ', err)
                return
            end

            local query = 'SELECT * FROM system.peers'
            local query_id, err = cluster:get_or_prepare(coordinator, query)
            if not query_id then
                ngx.log(ngx.ERR, 'could not prepare: ', err)
                return
            end

            local bit = require 'bit'
            ngx.say('0x'..bit.tohex(string.byte(query_id)))
        }
    }
--- request
GET /t
--- response_body_like
0x[\da-fA-F]{8}
--- no_error_log
[error]



=== TEST 2: cluster.get_or_prepare() sets shm and worker cache
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, 'could not refresh: ', err)
                return
            end

            local coordinator, err = cluster:next_coordinator()
            if not coordinator then
                ngx.log(ngx.ERR, 'could not get coordinator: ', err)
                return
            end

            local query = 'SELECT * FROM system.peers'
            local query_id, err = cluster:get_or_prepare(coordinator, query)
            if not query_id then
                ngx.log(ngx.ERR, 'could not prepare: ', err)
                return
            end

            ngx.say('worker cache is set: ', cluster.prepared_ids[query] == query_id)
            ngx.say('shm is set: ', cluster.shm:get('prepared:id:' .. query) == query_id)
        }
    }
--- request
GET /t
--- response_body
worker cache is set: true
shm is set: true
--- no_error_log
[error]



=== TEST 3: cluster.get_or_prepare() retrieves query id from worker cache
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, 'could not refresh: ', err)
                return
            end

            local coordinator, err = cluster:next_coordinator()
            if not coordinator then
                ngx.log(ngx.ERR, 'could not get coordinator: ', err)
                return
            end

            -- insert fixture in worker cache
            local query = 'SELECT * FROM system.peers'
            cluster.prepared_ids[query] = 'our prepared query id from cluster memory'

            local query_id, err = cluster:get_or_prepare(coordinator, query)
            if not query_id then
                ngx.log(ngx.ERR, 'could not prepare: ', err)
                return
            end

            ngx.say(query_id)
        }
    }
--- request
GET /t
--- response_body
our prepared query id from cluster memory
--- no_error_log
[error]



=== TEST 4: cluster.get_or_prepare() retrieves query_id from shm if not in worker cache
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, 'could not refresh: ', err)
                return
            end

            local coordinator, err = cluster:next_coordinator()
            if not coordinator then
                ngx.log(ngx.ERR, 'could not get coordinator: ', err)
                return
            end

            local query = 'SELECT * FROM system.peers'
            -- erase worker cache
            cluster.prepared_ids[query] = nil

            -- insert fixture in shm
            local ok, err = cluster.shm:set('prepared:id:' .. query, 'our query id from shm')
            if not ok then
                ngx.log(ngx.ERR, 'could not insert fixture: ', err)
                return
            end

            local query_id, err = cluster:get_or_prepare(coordinator, query)
            if not query_id then
                ngx.log(ngx.ERR, 'could not prepare: ', err)
                return
            end

            ngx.say(query_id)
        }
    }
--- request
GET /t
--- response_body
our query id from shm
--- no_error_log
[error]
