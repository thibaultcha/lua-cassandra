use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

plan tests => repeat_each() * blocks() * 3;

my $pwd = cwd();

our $HttpConfig = <<_EOC_;
    lua_package_path "$pwd/src/?.lua;;";
_EOC_

our $SpawnCluster = <<_EOC_;
    lua_shared_dict cassandra 1m;
    init_by_lua '
        local cassandra = require "cassandra"
        local ok, err = cassandra.spawn_cluster({
            shm = "cassandra",
            contact_points = {"127.0.0.1", "127.0.0.2"}
        })
        if not ok then
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
