# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;
use t::Util;

our $HttpConfig = $t::Util::HttpConfig;

plan tests => repeat_each() * blocks() * 6;

log_level('debug');

run_tests();

__DATA__

=== TEST 1: logging enabled by default
--- log_level: debug
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

            cluster:set_peer_down('127.0.0.1')
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr{\[warn\] .*? setting host at 127\.0\.0\.1 DOWN}
--- no_error_log
[error]
[info]
[debug]



=== TEST 2: opts.silent disables all logging
--- log_level: debug
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                silent = true
            }
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            cluster:set_peer_down('127.0.0.1')
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]
[warn]
[info]
[debug]
