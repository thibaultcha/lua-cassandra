use Test::Nginx::Socket::Lua;
use t::Utils;

log_level('error');

plan tests => repeat_each() * blocks() * 3 + 3;

run_tests();

__DATA__

=== TEST 1: new session
--- log_level: debug
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local dict = ngx.shared.cassandra
            local cassandra = require 'cassandra'
            local cluster, err = cassandra.new {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            ngx.say(dict:get("hosts"))
            ngx.say(dict:get("127.0.0.1"))
        }
    }
--- request
GET /t
--- response_body
127.0.0.1
0;0
--- no_error_log
[error]
--- error_log eval
qr/\[debug\].*?cluster infos retrieved in shm cassandra/



=== TEST 2: new session should iterate over contact_points
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require 'cassandra'
            local cluster, err = cassandra.new {
                shm = 'cassandra',
                contact_points = {"0.0.0.1", "0.0.0.2", "0.0.0.3", "127.0.0.1"}
            }
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            local dict = ngx.shared.cassandra
            ngx.say(dict:get("hosts"))
            ngx.say(dict:get("127.0.0.1"))
        }
    }
--- request
GET /t
--- response_body
127.0.0.1
0;0
--- error_log eval
[
    qr/.*?connect\(\) to 0\.0\.0\.1:9042 failed.*?/,
    qr/.*?connect\(\) to 0\.0\.0\.2:9042 failed.*?/,
    qr/.*?connect\(\) to 0\.0\.0\.3:9042 failed.*?/,
]



=== TEST 3: new session should accept custom port
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.new {
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



=== TEST 4: spawn session in keyspace
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.new {
                shm = "cassandra",
                keyspace = "system",
                contact_points = {"127.0.0.1"}
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



=== TEST 5: new session without contact_points
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.new {
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

--- error_log: contact_points is required



=== TEST 6: session:set_keyspace()
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.new {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            local ok, err = session:set_keyspace "system"
            if not ok then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            local rows, err = session:execute "SELECT key FROM local"
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



=== TEST 7: session:shutdown()
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.new {
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



=== TEST 8: session:set_keep_alive()
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.new {
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

            session:set_keep_alive()

            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            ngx.exit(200)
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 9: session:set_keep_alive() with pool timeout option
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session, err = cassandra.new {
                shm = "cassandra",
                contact_points = {"127.0.0.1"},
                socket_options = {
                    pool_timeout = 60
                }
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

            session:set_keep_alive()

            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            ngx.exit(200)
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 10: session:set_keep_alive() with pool size option
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            -- It should ignore it since ngx_lua cannot accept
            -- a nil arg #1
            local session = cassandra.new {
                shm = "cassandra",
                contact_points = {"127.0.0.1"},
                socket_options = {
                    pool_size = 25
                }
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

            session:set_keep_alive()

            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            ngx.exit(200)
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 11: session:set_keep_alive() with pool size and pool timeout options
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            -- It should ignore it since ngx_lua cannot accept
            -- a nil arg #1
            local session = cassandra.new {
                shm = "cassandra",
                contact_points = {"127.0.0.1"},
                socket_options = {
                    pool_timeout = 60,
                    pool_size = 25
                }
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

            session:set_keep_alive()

            local rows, err = session:execute "SELECT key FROM system.local"
            if err then
                ngx.log(ngx.ERR, err)
                ngx.exit(500)
            end

            ngx.exit(200)
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]
