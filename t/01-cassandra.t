use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

plan tests => repeat_each() * blocks() * 3;

my $pwd = cwd();

our $HttpConfig = <<_EOC_;
    lua_package_path "$pwd/src/?.lua;$pwd/src/?/init.lua;;";
_EOC_

our $SpawnCluster = <<_EOC_;
    lua_shared_dict cassandra 1m;
    lua_shared_dict cassandra_prepared 1m;
    init_by_lua '
        local cassandra = require "cassandra"
        local cluster, err = cassandra.spawn_cluster {
            shm = "cassandra",
            contact_points = {"127.0.0.1"}
        }
        if err then
            ngx.log(ngx.ERR, tostring(err))
        end
    ';
_EOC_

run_tests();

__DATA__

=== TEST 1: spawn cluster
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        return 200;
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 2: spawn session
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {shm = "cassandra"}
        ';
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 3: session:execute()
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {shm = "cassandra"}
            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            else
                ngx.say("type: "..rows.type)
                ngx.say("#rows: "..#rows)
                for _, row in ipairs(rows) do
                    ngx.say(row["key"])
                end
            end
        ';
    }
--- request
GET /t
--- response_body
type: ROWS
#rows: 1
local
--- no_error_log
[error]



=== TEST 4: session:execute() with request arguments
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {shm = "cassandra"}
            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            else
                ngx.say("type: "..rows.type)
                ngx.say("#rows: "..#rows)
                for _, row in ipairs(rows) do
                    ngx.say(row["key"])
                end
            end
        ';
    }
--- request
GET /t
--- response_body
type: ROWS
#rows: 1
local
--- no_error_log
[error]



=== TEST 5: wait for schema consensus
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                socket_options = {
                    connect_timeout = 5000,
                    read_timeout = 10000
                }
            }
            local res, err = session:execute [[
                CREATE KEYSPACE IF NOT EXISTS resty_t_keyspace
                WITH REPLICATION = {\'class\': \'SimpleStrategy\', \'replication_factor\': 1}
            ]]
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            res, err = session:execute [[
                CREATE TABLE IF NOT EXISTS resty_t_keyspace.users(
                    id uuid PRIMARY KEY,
                    name text
                )
            ]]
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            res, err = session:execute("DROP KEYSPACE resty_t_keyspace")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end
        ';
    }
--- request
GET /t
--- response_body

--- timeout: 5s
--- no_error_log
[error]



=== TEST 6: session:shutdown()
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {shm = "cassandra"}
            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            session:shutdown()

            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                return ngx.exit(200)
            end

            ngx.exit(500)
        ';
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[error\].*?Cannot reuse a session that has been shut down./



=== TEST 7: session:set_keep_alive()
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {shm = "cassandra"}
            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            session:set_keep_alive()

            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            ngx.exit(200)
        ';
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 8: session:set_keep_alive() with pool timeout option
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                socket_options = {
                    pool_timeout = 60
                }
            }
            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            session:set_keep_alive()

            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            ngx.exit(200)
        ';
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 9: session:set_keep_alive() with pool size option
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            -- It should ignore it since ngx_lua cannot accept
            -- a nil arg #1
            local session = cassandra.spawn_session {
                shm = "cassandra",
                socket_options = {
                    pool_size = 25
                }
            }
            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            session:set_keep_alive()

            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            ngx.exit(200)
        ';
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 10: session:set_keep_alive() with pool size and pool timeout options
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        content_by_lua '
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
            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            session:set_keep_alive()

            local rows, err = session:execute("SELECT key FROM system.local")
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

            ngx.exit(200)
        ';
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 11: session:execute() prepared query
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {shm = "cassandra", prepared_shm = "cassandra_prepared"}

            for i = 1, 10 do
                local rows, err = session:execute("SELECT key FROM system.local", nil, {prepare = true})
                if err then
                    ngx.log(ngx.ERR, tostring(err))
                    ngx.exit(500)
                end
                ngx.say(rows[1].key)
            end
        ';
    }
--- request
GET /t
--- response_body
local
local
local
local
local
local
local
local
local
local
--- no_error_log
[error]
