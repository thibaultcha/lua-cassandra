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
            local hosts = {"127.0.0.1", "127.0.0.2", "127.0.0.3"}

            for _, host in iter(shm, hosts) do
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



=== TEST 2: multiple shared round robin
--- http_config eval
"$::HttpConfig"
--- config
    location /t {
        content_by_lua '
            local iter = require("cassandra.policies.load_balancing").SharedRoundRobin
            local shm = "cassandra"
            local hosts = {"127.0.0.1", "127.0.0.2", "127.0.0.3"}

            local iter1 = iter(shm, hosts)
            local iter2 = iter(shm, hosts)
            local iter3 = iter(shm, hosts)

            ngx.say(select(2, iter1()))
            ngx.say(select(2, iter2()))
            ngx.say(select(2, iter3()))

            ngx.say(select(2, iter1()))
            ngx.say(select(2, iter1()))

            ngx.say(select(2, iter2()))
            ngx.say(select(2, iter3()))
            ngx.say(select(2, iter2()))
            ngx.say(select(2, iter3()))
        ';
    }
--- request
GET /t
--- response_body
127.0.0.1
127.0.0.2
127.0.0.3
127.0.0.2
127.0.0.3
127.0.0.3
127.0.0.1
127.0.0.1
127.0.0.2
--- no_error_log
[error]



=== TEST 3: handling missing index in shm
--- http_config eval
"$::HttpConfig"
--- config
    location /t {
        content_by_lua '
            local iter = require("cassandra.policies.load_balancing").SharedRoundRobin
            local shm = "cassandra"
            local hosts = {"127.0.0.1", "127.0.0.2", "127.0.0.3"}

            local iter1 = iter(shm, hosts)
            ngx.say(select(2, iter1()))

            local dict = ngx.shared[shm]
            dict:delete("rr_index")

            iter1 = iter(shm, hosts)
            ngx.say(select(2, iter1()))
        ';
    }
--- request
GET /t
--- response_body
127.0.0.1
127.0.0.1
--- no_error_log
[error]



=== TEST 4: handling invalid index in shm
--- http_config eval
"$::HttpConfig"
--- config
    location /t {
        content_by_lua '
            local iter = require("cassandra.policies.load_balancing").SharedRoundRobin
            local shm = "cassandra"
            local hosts = {"127.0.0.1", "127.0.0.2", "127.0.0.3"}

            local iter1 = iter(shm, hosts)
            ngx.say(select(2, iter1()))

            local dict = ngx.shared[shm]
            local ok, err = dict:replace("rr_index", "hello")
            if not ok then
              ngx.say(err)
              ngx.exit(500)
            end

            iter1 = iter(shm, hosts)
            ngx.say(select(2, iter1()))
        ';
    }
--- request
GET /t
--- response_body
127.0.0.1
127.0.0.1
--- error_log eval
qr/\[error\].*?Cannot increment shared round robin load balancing policy index in shared dict cassandra: not a number/
