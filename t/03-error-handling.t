use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(2);

plan tests => repeat_each() * blocks() * 5;

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

=== TEST 1: shm cluster info disapeared
--- http_config eval
"$::HttpConfig
 $::SpawnCluster"
--- config
    location /t {
      content_by_lua '
        local cassandra = require "cassandra"
        local cache = require "cassandra.cache"
        local shm = "cassandra"

        local dict = ngx.shared[shm]
        local hosts, err = cache.get_hosts(shm)
        if err then
          ngx.log(ngx.ERR, tostring(err))
          ngx.exit(500)
        elseif hosts == nil or #hosts < 1 then
          ngx.log(ngx.ERR, "no hosts set in shm")
          ngx.exit(500)
        end

        -- erase hosts from the cache
        dict:delete("hosts")

        local hosts, err = cache.get_hosts(shm)
        if err then
          ngx.log(ngx.ERR, tostring(err))
          ngx.exit(500)
        elseif hosts ~= nil then
          ngx.log(ngx.ERR, "hosts set in shm after delete")
          ngx.exit(500)
        end

        -- attempt to create session
        local session, err = cassandra.spawn_session {
          shm = shm,
          contact_points = {"127.0.0.1"} -- safe contact point just in case
        }
        if err then
          ngx.log(ngx.ERR, tostring(err))
          ngx.exit(500)
        end

        -- attempt query
        local rows, err = session:execute("SELECT * FROM system.local")
        if err then
          ngx.log(ngx.ERR, tostring(err))
          ngx.exit(500)
        end

        ngx.say(#rows)
        ngx.say(rows[1].key)
      ';
    }
--- request
GET /t
--- response_body
1
local
--- no_error_log
[error]
--- error_log eval
[
    qr/\[warn\].*?No cluster infos in shared dict/,
    qr/\[info\].*?Cluster infos retrieved in shared dict cassandra/
]
