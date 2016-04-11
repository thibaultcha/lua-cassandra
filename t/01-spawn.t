use Test::Nginx::Socket::Lua;
use t::Utils;

log_level('error');

plan tests => repeat_each() * blocks() * 3 + (repeat_each() * 2);

run_tests();

__DATA__

=== TEST 1: spawn cluster
--- http_config eval
"$t::Utils::HttpConfig
init_by_lua_block {
    local cassandra = require 'cassandra'
    local cluster, err = cassandra.spawn_cluster {
        shm = 'cassandra',
        contact_points = {'127.0.0.1'}
    }
    if err then
        ngx.log(ngx.ERR, err)
    end
}"
--- config
    location /t {
        content_by_lua_block {
            local dict = ngx.shared.cassandra
            ngx.say(dict:get("hosts"))
            ngx.say(dict:get("127.0.0.1:9042"))
        }
    }
--- request
GET /t
--- response_body
127.0.0.1:9042
0;0
--- no_error_log
[error]



=== TEST 2: spawn cluster should iterate over contact_points
--- http_config eval
"$t::Utils::HttpConfig
init_by_lua_block {
    local cassandra = require 'cassandra'
    local cluster, err = cassandra.spawn_cluster {
        shm = 'cassandra',
        contact_points = {'0.0.0.1', '0.0.0.2', '0.0.0.3', '127.0.0.1'}
    }
    if err then
        ngx.log(ngx.ERR, err)
    end
}"
--- config
    location /t {
        content_by_lua_block {
            local dict = ngx.shared.cassandra
            ngx.say(dict:get("hosts"))
            ngx.say(dict:get("127.0.0.1:9042"))
        }
    }
--- request
GET /t
--- response_body
127.0.0.1:9042
0;0
--- no_error_log
[error]



=== TEST 3: spawn cluster should accept custom port
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local cluster, err = cassandra.spawn_cluster {
                shm = "cassandra",
                contact_points = {"127.0.0.1:9043"},
                socket_options = {
                    connect_timeout = 500
                }
            }
            if err then
                ngx.log(ngx.ERR, err)
            end
        }
    }
--- request
GET /t
--- response_body

--- timeout: 1.5
--- error_log eval
qr/\[error\].*?all hosts tried for query failed\. 127\.0\.0\.1:9043/



=== TEST 4: spawn session without contact_points if cluster spawned
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.spawn_session {shm = "cassandra"}
            if err or session == nil then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 5: spawn session in keyspace
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.spawn_session {
                shm = "cassandra",
                keyspace = "system"
            }
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            local rows, err = session:execute "SELECT * FROM local"
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            ngx.say("type: "..rows.type)
            ngx.say("#rows: "..#rows)
            for _, row in ipairs(rows) do
                ngx.say(row["key"])
            end
        }
    }
--- request
GET /t
--- response_body
type: ROWS
#rows: 1
local
--- no_error_log
[error]



=== TEST 6: spawn session without cluster with contact_points
--- log_level: debug
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.spawn_session {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            ngx.say("type: "..rows.type)
            ngx.say("#rows: "..#rows)
            for _, row in ipairs(rows) do
                ngx.say(row["key"])
            end
        }
    }
--- request
GET /t
--- response_body
type: ROWS
#rows: 1
local
--- no_error_log
[error]
--- error_log eval
[
    qr/\[warn\].*?no cluster infos in shared dict/,
    qr/\[debug\].*?cluster infos retrieved in shared dict cassandra/
]



=== TEST 7: spawn session without cluster nor contact_points
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.spawn_session {
                shm = "cassandra"
            }
            if err then
                ngx.log(ngx.ERR, err)
            end
        }
    }
--- request
GET /t
--- response_body

--- error_log: option error: options must contain contact_points to spawn session or cluster
