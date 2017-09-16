# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;

our $HttpConfig = <<_EOC_;
    lua_package_path './lib/?.lua;./lib/?/init.lua;;';
_EOC_

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: cql exposes its constants
--- http_config eval: $::HttpConfig
--- config
    location = /t {
        content_by_lua_block {
            local cql = require "cassandra.cql2"

            ngx.say("min protocol version: ", cql.MIN_PROTOCOL_VERSION)
            ngx.say("default protocol version: ", cql.DEFAULT_PROTOCOL_VERSION)
            ngx.say("TYP_UNSET: ", type(cql.TYP_UNSET))
            ngx.say("TYP_NULL: ", type(cql.TYP_NULL))
        }
    }
--- request
GET /t
--- response_body_like
min protocol version: \d+
default protocol version: \d+
TYP_UNSET: table
TYP_NULL: table
--- no_error_log
[error]
