use Test::Nginx::Socket::Lua;
use t::Utils;

log_level('error');

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: session:set_keyspace()
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {shm = "cassandra"}

            local ok, err = session:set_keyspace "system"
            if not ok then
                ngx.log(ngx.ERR, err)
            end

            local rows, err = session:execute "SELECT key FROM local"
            if err then
                ngx.log(ngx.ERR, err)
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



=== TEST 2: session:shutdown()
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {shm = "cassandra"}
            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            session:shutdown()

            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
                return ngx.exit(200)
            end

            ngx.exit(500)
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[error\].*?cannot reuse a session that has been shut down/



=== TEST 3: session:set_keep_alive()
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {shm = "cassandra"}
            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
            end

            session:set_keep_alive()

            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
            end

            ngx.exit(200)
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 4: session:set_keep_alive() with pool timeout option
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                socket_options = {
                    pool_timeout = 60
                }
            }
            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
            end

            session:set_keep_alive()

            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
            end

            ngx.exit(200)
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 5: session:set_keep_alive() with pool size option
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            -- It should ignore it since ngx_lua cannot accept
            -- a nil arg #1
            local session = cassandra.spawn_session {
                shm = "cassandra",
                socket_options = {
                    pool_size = 25
                }
            }
            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
            end

            session:set_keep_alive()

            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
            end

            ngx.exit(200)
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 6: session:set_keep_alive() with pool size and pool timeout options
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            -- It should ignore it since ngx_lua cannot accept
            -- a nil arg #1
            local session = cassandra.spawn_session {
                shm = "cassandra",
                socket_options = {
                    pool_timeout = 60,
                    pool_size = 25
                }
            }
            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
            end

            session:set_keep_alive()

            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
            end

            ngx.exit(200)
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]
