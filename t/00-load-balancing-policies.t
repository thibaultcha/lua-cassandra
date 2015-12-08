use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

repeat_each(1);

plan tests => repeat_each() * blocks() * 3;

my $pwd = cwd();

our $HttpConfig = <<_EOC_;
    lua_package_path "$pwd/src/?.lua;$pwd/src/?/init.lua;;";
    lua_shared_dict cassandra 1m;
_EOC_

run_tests();

__DATA__

=== TEST 1: shared round robin
--- http_config eval
"$::HttpConfig"
--- config
    location /t {
        content_by_lua '
            local iter = require("cassandra.policies.load_balancing").SharedRoundRobin
            local shm = "cassandra"
            local hosts_fixtures = {"127.0.0.1", "127.0.0.2", "127.0.0.3"}

            for _, host in iter(shm, hosts_fixtures) do
                ngx.say(host)
            end
        ';
    }
--- request
GET /t
--- response_body
127.0.0.1
127.0.0.2
127.0.0.3
--- no_error_log
[error]
