# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;

our $HttpConfig = <<_EOC_;
    lua_package_path 'lib/?.lua;lib/?/init.lua;;';
    lua_shared_dict cassandra 1m;
_EOC_

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: cluster.execute() sanity
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.say(err)
            end

            local rows, err = cluster:execute("SELECT * FROM system.peers")
            if not rows then
                ngx.log(ngx.ERR, err)
            end

            ngx.say(#rows)
        }
    }
--- request
GET /t
--- response_body
2
--- no_error_log
[error]
