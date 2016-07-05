# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;

our $HttpConfig = <<_EOC_;
    lua_package_path 'lib/?.lua;lib/?/init.lua;;';
    lua_shared_dict cassandra 1m;
_EOC_

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: handle_error() re-prepares UNPREPARED errors
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

            math.randomseed(ngx.now()*1000)
            local r = math.random(10^8)
            local query = "SELECT * FROM system.local WHERE key = '"..r.."'"

            for i = 1, 3 do
                local rows, err = cluster:execute(query, nil, {prepared = true})
                if not rows then
                    ngx.log(ngx.ERR, err)
                    return
                end
            end
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr{\[notice\] .*? preparing and retrying}



=== TEST 2: handle_error() retries on appropriate CQL errors
--- http_config eval
qq{
    $::HttpConfig
    init_by_lua_block {
        local Cluster = require 'resty.cassandra.cluster'
        Cluster.send_retry = function()
            return 'retried'
        end
        Cluster.coordinator_fixture = {
            setkeepalive = function()end
        }
    }
}
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require 'cassandra'
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            for _, k in ipairs({'OVERLOADED', 'IS_BOOTSTRAPPING', 'TRUNCATE_ERROR'}) do
                local res, err, code = cluster:handle_error('some CQL error',
                                                            cassandra.cql_errors[k],
                                                            cluster.coordinator_fixture)
                ngx.say(k, ': ', res, ' ', err, ' ', code)
            end
        }
    }
--- request
GET /t
--- response_body
OVERLOADED: retried nil nil
IS_BOOTSTRAPPING: retried nil nil
TRUNCATE_ERROR: retried nil nil
--- no_error_log
[error]



=== TEST 3: handle_error() uses retry policy
--- http_config eval
qq{
    $::HttpConfig
    init_by_lua_block {
        local Cluster = require 'resty.cassandra.cluster'
        Cluster.send_retry = function()
            return 'retried'
        end
        Cluster.coordinator_fixture = {
            setkeepalive = function()end
        }
    }
}
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require 'cassandra'
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local res, err, code = cluster:handle_error('some CQL error',
                                                        cassandra.cql_errors.UNAVAILABLE_EXCEPTION,
                                                        cluster.coordinator_fixture,
                                                        {retries = 10})
            ngx.say('UNAVAILABLE_EXCEPTION: ', res, ' ', err, ' ', code)

            local res, err, code = cluster:handle_error('some CQL error',
                                                        cassandra.cql_errors.READ_TIMEOUT,
                                                        cluster.coordinator_fixture,
                                                        {retries = 0})
            ngx.say('READ_TIMEOUT retry: ', res, ' ', err, ' ', code)

            local res, err, code = cluster:handle_error('some CQL error',
                                                        cassandra.cql_errors.READ_TIMEOUT,
                                                        cluster.coordinator_fixture,
                                                        {retries = 10})
            ngx.say('READ_TIMEOUT throw: ', res, ' ', err, ' ', code)

            local res, err, code = cluster:handle_error('some CQL error',
                                                        cassandra.cql_errors.WRITE_TIMEOUT,
                                                        cluster.coordinator_fixture,
                                                        {retries = 0})
            ngx.say('WRITE_TIMEOUT retry: ', res, ' ', err, ' ', code)

            local res, err, code = cluster:handle_error('some CQL error',
                                                        cassandra.cql_errors.WRITE_TIMEOUT,
                                                        cluster.coordinator_fixture,
                                                        {retries = 10})
            ngx.say('WRITE_TIMEOUT throw: ', res, ' ', err, ' ', code)
        }
    }
--- request
GET /t
--- response_body
UNAVAILABLE_EXCEPTION: nil some CQL error 4096
READ_TIMEOUT retry: retried nil nil
READ_TIMEOUT throw: nil some CQL error 4608
WRITE_TIMEOUT retry: retried nil nil
WRITE_TIMEOUT throw: nil some CQL error 4352
--- no_error_log
[error]



=== TEST 4: retry_on_timeout cluster option
--- http_config eval
qq{
    $::HttpConfig
    init_by_lua_block {
        local Cluster = require 'resty.cassandra.cluster'
        Cluster.send_retry = function()
            return 'retried'
        end
        Cluster.coordinator_fixture = {
            setkeepalive = function()end,
            host = '127.0.0.1'
        }
    }
}
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require 'cassandra'
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local res, err, code = cluster:handle_error('timeout', nil, Cluster.coordinator_fixture)
            ngx.say(res, ' ', err, ' ', code)

            cluster, err = Cluster.new({retry_on_timeout = false})
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            res, err, code = cluster:handle_error('timeout', nil, Cluster.coordinator_fixture)
            ngx.say(res, ' ', err, ' ', code)
        }
    }
--- request
GET /t
--- response_body
retried nil nil
nil timeout nil
--- no_error_log
[error]



=== TEST 5: sets host down if unresponsive
--- http_config eval
qq{
    $::HttpConfig
    init_by_lua_block {
        local Cluster = require 'resty.cassandra.cluster'
        Cluster.send_retry = function()
            return 'retried'
        end
        Cluster.coordinator_fixture = {
            setkeepalive = function()end,
            host = '127.0.0.1'
        }
    }
}
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require 'cassandra'
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err = cluster:set_peer_up(Cluster.coordinator_fixture.host)
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            ok, err = cluster:can_try_peer(Cluster.coordinator_fixture.host)
            if err then
                ngx.log(ngx.ERR, err)
                return
            end
            ngx.say('is up: ', ok)

            local res, err, code = cluster:handle_error('closed', nil, Cluster.coordinator_fixture)
            ngx.say(res, ' ', err, ' ', code)

            ok, err = cluster:can_try_peer(Cluster.coordinator_fixture.host)
            if err then
                ngx.log(ngx.ERR, err)
                return
            end
            ngx.say('is up: ', ok)
        }
    }
--- request
GET /t
--- response_body
is up: true
retried nil nil
is up: false
--- no_error_log
[error]
