# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;
use t::Util;

our $HttpConfig = $t::Util::HttpConfig;

plan tests => repeat_each() * blocks() * 3 + 3;

no_shuffle();

run_tests();

__DATA__

=== TEST 1: cluster.iterate() sanity
--- timeout: 45
--- http_config eval
qq{
    $::HttpConfig
    init_worker_by_lua_block {
        local Cluster = require 'resty.cassandra.cluster'
        local cluster, err = Cluster.new {
            timeout_read = 10000
        }
        if not cluster then
            ngx.log(ngx.ERR, err)
            return
        end

        assert(cluster:execute [[
            CREATE KEYSPACE IF NOT EXISTS lua_resty_tests WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        ]])

        assert(cluster:execute [[
            CREATE TABLE IF NOT EXISTS lua_resty_tests.iterate(
                id int PRIMARY KEY,
                n int
            )
        ]])

        assert(cluster:execute "TRUNCATE lua_resty_tests.iterate")
        for i = 1, 101 do
          assert(cluster:execute("INSERT INTO lua_resty_tests.iterate(id,n) VALUES(?,?)", {i, i*i}))
        end
    }
}
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                keyspace = 'lua_resty_tests',
                 timeout_read = 10000
                }
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local opts = {page_size = 20}
            local buffer = {}
            local n_pages = 0
            for rows, err, page in cluster:iterate("SELECT * FROM iterate", nil, opts) do
              if err then
                ngx.log(ngx.ERR, 'could not fetch page: ', err)
                break
              end

              n_pages = n_pages + 1
              for _, row in ipairs(rows) do buffer[#buffer+1] = row end
            end

            ngx.say('pages: ', n_pages)
            ngx.say('n rows: ', #buffer)
        }
    }
--- request
GET /t
--- response_body
pages: 6
n rows: 101
--- no_error_log
[error]



=== TEST 2: cluster.iterate() uses load balancing policy
--- log_level: debug
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                keyspace = 'lua_resty_tests',
                timeout_read = 10000
            }
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local opts = {page_size = 40}
            for rows, err, page in cluster:iterate("SELECT * FROM iterate", nil, opts) do
              if err then
                ngx.log(ngx.ERR, 'could not fetch page: ', err)
                break
              end
            end
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]
--- error_log
next_coordinator(): [lua-cassandra] load balancing policy chose host at 127.0.0.3
next_coordinator(): [lua-cassandra] load balancing policy chose host at 127.0.0.2
next_coordinator(): [lua-cassandra] load balancing policy chose host at 127.0.0.1
