use Test::Nginx::Socket::Lua;
use t::Utils;

log_level('error');

repeat_each(2);

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: session:execute()
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
                    id uuid,
                    name varchar,
                    n int,
                    PRIMARY KEY(id, n)
                )
            ]]
            if err then
                ngx.log(ngx.ERR, err)
            end

            local _UUID = "ca002f0a-8fe4-11e5-9663-43d80ec97d3e"
            res, err = session:batch({
              {"INSERT INTO resty_t_keyspace.users(id, name, n) VALUES(".._UUID..", 'Alice', 1)"},
              {"UPDATE resty_t_keyspace.users SET name = 'Alice' WHERE id = ".._UUID.." AND n = 1"},
              {"UPDATE resty_t_keyspace.users SET name = 'Alicia' WHERE id = ".._UUID.." AND n = 1"}
            })
            if err then
                ngx.log(ngx.ERR, err)
            end

            local rows, err = session:execute([[
                SELECT * FROM resty_t_keyspace.users WHERE id = ? AND n = 1
            ]], {cassandra.uuid(_UUID)})
            if err then
                ngx.log(ngx.ERR, err)
            end

            for _, row in ipairs(rows) do
                ngx.say(row.name)
                ngx.say(row.n)
            end

            res, err = session:execute "DROP KEYSPACE resty_t_keyspace"
            if err then
                ngx.log(ngx.ERR, err)
            end
        }
    }
--- request
GET /t
--- response_body
Alicia
1
--- timeout: 5s
--- no_error_log
[error]
