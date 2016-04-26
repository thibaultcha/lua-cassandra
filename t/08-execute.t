# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;

our $HttpConfig = <<_EOC_;
    lua_package_path 'lib/?.lua;lib/?/init.lua;;';
    lua_shared_dict cassandra 1m;
_EOC_

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: refreshes peers if not init
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

            local rows, err = cluster:execute("SELECT * FROM system.peers")
            if not rows then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say('init: ', cluster.init)
            ngx.say(#rows)
        }
    }
--- request
GET /t
--- response_body
init: true
2
--- no_error_log
[error]



=== TEST 2: uses next_coordinator()
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

            for i = 1, 3 do
                local rows, err = cluster:execute("SELECT * FROM system.local")
                if not rows then
                    ngx.log(ngx.ERR, err)
                    return
                end

                ngx.say(rows[1].rpc_address)
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



=== TEST 3: returns CQL errors
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

            local rows, err = cluster:execute("SELECT")
            if not rows then
                ngx.say(err)
            end
        }
    }
--- request
GET /t
--- response_body
[Syntax error] line 0:-1 no viable alternative at input '<EOF>'
--- no_error_log
[error]



=== TEST 4: passes query args
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

            local q = 'SELECT * FROM system.local WHERE key = ?'
            local rows, err = cluster:execute(q, {'local'})
            if not rows then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say(rows[1].key)
        }
    }
--- request
GET /t
--- response_body
local
--- no_error_log
[error]



=== TEST 5: coordinator is spawned with cluster opts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                keyspace = 'system'
            }
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local rows, err = cluster:execute('SELECT * FROM local WHERE key = ?', {'local'})
            if not rows then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say(rows[1].key)
        }
    }
--- request
GET /t
--- response_body
local
--- no_error_log
[error]



=== TEST 6: opts.prepared: prepares a query
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

            local query = 'SELECT * FROM system.local'
            local rows, err = cluster:execute(query, nil, {prepared = true})
            if not rows then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say(rows[1].key)
            ngx.say('has cluster worker cache: ', cluster.prepared_ids[query] ~= nil)
            ngx.say('has shm cache: ', cluster.shm:get('prepared:id:' .. query) ~= nil)

        }
    }
--- request
GET /t
--- response_body
local
has cluster worker cache: true
has shm cache: true
--- no_error_log
[error]



=== TEST 7: opts.prepared: returns errors
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

            local query = 'SELECT'
            local rows, err = cluster:execute(query, nil, {prepared = true})
            if not rows then
                ngx.say(err)
            end
        }
    }
--- request
GET /t
--- response_body
could not prepare query: [Syntax error] line 0:-1 no viable alternative at input '<EOF>'
--- no_error_log
[error]
