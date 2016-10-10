# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;
use t::Util;

our $HttpConfig = $t::Util::HttpConfig;

plan tests => repeat_each() * blocks() * 3 + 1;

no_shuffle();

run_tests();

__DATA__

=== TEST 1: cluster.batch() passes options to host module (counter update)
--- timeout: 30
--- http_config eval
qq{
    $::HttpConfig
    init_worker_by_lua_block {
        local Cluster = require 'resty.cassandra.cluster'
        local cluster, err = Cluster.new()
        if not cluster then
            ngx.log(ngx.ERR, err)
            return
        end

        assert(cluster:execute [[
            CREATE KEYSPACE IF NOT EXISTS lua_resty_tests WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        ]])

        assert(cluster:execute [[
            CREATE TABLE IF NOT EXISTS lua_resty_tests.metrics(
                id text PRIMARY KEY,
                n counter
            )
        ]])
    }
}
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                keyspace = 'lua_resty_tests'
            }
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local rows, err = cluster:execute('SELECT n FROM metrics WHERE id = ?', {
                'batch'
            })
            if not rows then
                ngx.log(ngx.ERR, err)
                return
            end

            local n = #rows > 0 and rows[1].n or 0

            local b = {
                {"UPDATE metrics SET n = n + 1 WHERE id = 'batch'"},
                {"UPDATE metrics SET n = n + 2 WHERE id = ?", {'batch'}}
            }

            local res, err = cluster:batch(b, {
                counter = true -- fail if this isn't passed to host module
            })
            if not res then
                ngx.log(ngx.ERR, err)
                return
            end

            rows, err = cluster:execute('SELECT n FROM metrics WHERE id = ?', {
                'batch'
            })
            if not rows then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say(rows[1].n == n + 3)
        }
    }
--- request
GET /t
--- response_body
true
--- no_error_log
[error]



=== TEST 2: cluster.batch() prepared queries
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                keyspace = 'lua_resty_tests'
            }
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local rows, err = cluster:execute('SELECT n FROM metrics WHERE id = ?', {
                'batch'
            })
            if not rows then
                ngx.log(ngx.ERR, err)
                return
            end

            local n = #rows > 0 and rows[1].n or 0

            local b = {
                {"UPDATE metrics SET n = n + 1 WHERE id = 'batch'"},
                {"UPDATE metrics SET n = n + 2 WHERE id = ?", {'batch'}}
            }

            local res, err = cluster:batch(b, {
                counter = true,
                prepared = true
            })
            if not res then
                ngx.log(ngx.ERR, err)
                return
            end

            rows, err = cluster:execute('SELECT n FROM metrics WHERE id = ?', {
                'batch'
            })
            if not rows then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say(rows[1].n == n + 3)
            local k1 = b[1][1]
            local k2 = b[2][1]
            ngx.say('query_id 1: ', cluster.prepared_ids[k1] ~= nil)
            ngx.say('query_id 2: ', cluster.prepared_ids[k2] ~= nil)
        }
    }
--- request
GET /t
--- response_body
true
query_id 1: true
query_id 2: true
--- no_error_log
[error]



=== TEST 3: cluster.batch() handles unprepared queries
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                keyspace = 'lua_resty_tests'
            }
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local rows, err = cluster:execute('SELECT n FROM metrics WHERE id = ?', {
                'batch'
            })
            if not rows then
                ngx.log(ngx.ERR, err)
                return
            end

            local n = #rows > 0 and rows[1].n or 0

            math.randomseed(ngx.now()*1000)
            local r = math.random(10^5)
            local b = {
                {"UPDATE metrics SET n = n + "..r.." WHERE id = 'batch'"},
                {"UPDATE metrics SET n = n + "..r.." WHERE id = ?", {'batch'}}
            }

            for i = 1, 3 do
                local res, err = cluster:batch(b, {
                    counter = true,
                    prepared = true
                })
                if not res then
                    ngx.log(ngx.ERR, err)
                    return
                end
            end

            rows, err = cluster:execute('SELECT n FROM metrics WHERE id = ?', {
                'batch'
            })
            if not rows then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say(rows[1].n == n + r*6)
            local k1 = b[1][1]
            local k2 = b[2][1]
            ngx.say('query_id 1: ', cluster.prepared_ids[k1] ~= nil)
            ngx.say('query_id 2: ', cluster.prepared_ids[k2] ~= nil)
        }
    }
--- request
GET /t
--- response_body
true
query_id 1: true
query_id 2: true
--- error_log eval
qr{\[notice\] .*? some requests from this batch were not prepared on host}
--- no_error_log
[error]
