use Test::Nginx::Socket::Lua;
use t::Utils;

log_level('error');

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: session:execute() raw query
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



=== TEST 2: session:execute() query with args binding
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {shm = "cassandra"}
            local rows, err = session:execute("SELECT * FROM system.local WHERE key = ?", {"local"})
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



=== TEST 3: session:execute() wait for schema consensus on SCHEMA_CHANGE
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
                    connect_timeout = 5000,
                    read_timeout = 10000
                }
            }
            local res, err = session:execute [[
                CREATE KEYSPACE IF NOT EXISTS resty_t_keyspace
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            ]]
            if err then
                ngx.log(ngx.ERR, err)
            end

            res, err = session:execute [[
                CREATE TABLE IF NOT EXISTS resty_t_keyspace.users(
                    id uuid PRIMARY KEY,
                    name text
                )
            ]]
            if err then
                ngx.log(ngx.ERR, err)
            end

            res, err = session:execute "INSERT INTO resty_t_keyspace.users(id, name) VALUES(uuid(), 'john')"
            if err then
                ngx.log(ngx.ERR, err)
            end

            local rows, err = session:execute "SELECT * FROM resty_t_keyspace.users"
            if err then
                ngx.log(ngx.ERR, err)
            end

            ngx.say("#rows: "..#rows)
            ngx.say(rows[1].name)

            res, err = session:execute "DROP KEYSPACE resty_t_keyspace"
            if err then
                ngx.log(ngx.ERR, err)
            end
        }
    }
--- request
GET /t
--- response_body
#rows: 1
john
--- timeout: 5s
--- no_error_log
[error]



=== TEST 4: session:execute() args serializers
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
                    connect_timeout = 5000,
                    read_timeout = 10000
                }
            }
            local res, err = session:execute [[
                CREATE KEYSPACE IF NOT EXISTS resty_t_keyspace
                WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
            ]]
            if err then
                ngx.log(ngx.ERR, err)
            end

            res, err = session:execute [[
                CREATE TABLE IF NOT EXISTS resty_t_keyspace.users(
                    id uuid PRIMARY KEY,
                    name text
                )
            ]]
            if err then
                ngx.log(ngx.ERR, err)
            end

            local _UUID = "ca002f0a-8fe4-11e5-9663-43d80ec97d3e"
            res, err = session:execute([[
                INSERT INTO resty_t_keyspace.users(id, name) VALUES(?, 'john')
            ]], {cassandra.uuid(_UUID)})
            if err then
                ngx.log(ngx.ERR, err)
            end

            local rows, err = session:execute([[
                SELECT * FROM resty_t_keyspace.users WHERE id = ?
            ]], {cassandra.uuid(_UUID)})
            if err then
                ngx.log(ngx.ERR, err)
            end

            ngx.say("#rows: "..#rows)
            ngx.say(rows[1].name)

            res, err = session:execute "DROP KEYSPACE resty_t_keyspace"
            if err then
                ngx.log(ngx.ERR, err)
            end
        }
    }
--- request
GET /t
--- response_body
#rows: 1
john
--- timeout: 5s
--- no_error_log
[error]



=== TEST 5: session:execute() prepared query
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                prepared_shm = "cassandra_prepared"
            }

            for i = 1, 10 do
                local rows, err = session:execute("SELECT key FROM system.local WHERE key = ?", {"local"}, {prepare = true})
                if err then
                    ngx.log(ngx.ERR, err)
                end
                ngx.say(rows[1].key)
            end
        }
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



=== TEST 6: session:execute() prepared query without prepared_shm
--- log_level: warn
--- http_config eval
"$t::Utils::HttpConfig
 $t::Utils::SpawnCluster"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra"
            }

            for i = 1, 10 do
                local rows, err = session:execute("SELECT key FROM system.local WHERE key = ?", {"local"}, {prepare = true})
                if err then
                    ngx.log(ngx.ERR, err)
                    ngx.exit(500)
                end
                ngx.say(rows[1].key)
            end
        }
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
--- error_log eval
qr/\[warn\].*?same shm used for cluster infos and prepared statements, consider using different ones/
