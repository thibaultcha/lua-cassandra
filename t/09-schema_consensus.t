# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;
use t::Util;

our $HttpConfig = $t::Util::HttpConfig;

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: cluster.execute() waits for schema consensus on DDL
--- timeout: 30
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                timeout_read = 5000
            }
            if not cluster then
                ngx.say(err)
            end

            math.randomseed(ngx.now()*1000)
            local name = 'consensus_'..math.random(0, 10^5)

            local create_q = string.format([[
                CREATE KEYSPACE %s WITH REPLICATION = {
                    'class': 'SimpleStrategy',
                    'replication_factor': 1
                }
            ]], name)

            local drop_q = string.format('DROP KEYSPACE %s', name)

            local res, err = cluster:execute(create_q)
            if not res then
                ngx.log(ngx.ERR, 'could not create: ', err)
                return
            end

            ngx.say(res.type)
            ngx.say(res.schema_version)

            res, err = cluster:execute(drop_q)
            if not res then
                ngx.log(ngx.ERR, 'could not drop: ', err)
                return
            end

            ngx.say(res.type)
            ngx.say(res.schema_version)
        }
    }
--- request
GET /t
--- response_body_like
SCHEMA_CHANGE
[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}
SCHEMA_CHANGE
[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}
--- no_error_log
[error]
