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
qr/\[notice\] .*? preparing and retrying/
