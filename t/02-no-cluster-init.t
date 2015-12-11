use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

plan tests => repeat_each() * blocks() * 4;

my $pwd = cwd();

our $HttpConfig = <<_EOC_;
    lua_package_path "$pwd/src/?.lua;$pwd/src/?/init.lua;;";
    lua_shared_dict cassandra 1m;
_EOC_

run_tests();

__DATA__

=== TEST 1: spawn session without cluster
--- http_config eval
"$::HttpConfig"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            local session, err = cassandra.spawn_session {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }
            if err then
                ngx.log(ngx.ERR, tostring(err))
                ngx.exit(500)
            end

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
--- error_log eval
[
    qr/\[warn\].*?No cluster infos in shared dict/,
    qr/\[info\].*?Cluster infos retrieved in shared dict cassandra/
]



=== TEST 2: spawn session without cluster nor contact_points option
--- http_config eval
"$::HttpConfig"
--- config
    location /t {
        content_by_lua '
            local cassandra = require "cassandra"
            local session, err = cassandra.spawn_session {
                shm = "cassandra"
            }
            if err then
                ngx.log(ngx.ERR, tostring(err))
            end
        ';
    }
--- request
GET /t
--- response_body

--- error_log: Options must contain contact_points to spawn session, or spawn a cluster in the init phase.
