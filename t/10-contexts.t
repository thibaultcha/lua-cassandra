use Test::Nginx::Socket::Lua;
use t::Utils;

log_level('error');

repeat_each(3);

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: support in init
--- http_config eval
"$t::Utils::HttpConfig
lua_shared_dict test 128k;
init_by_lua_block {
    local dict = ngx.shared.test
    local cassandra = require 'cassandra'
    local session = cassandra.spawn_session {
        shm = 'cassandra',
        contact_points = {'127.0.0.1'}
    }

    local rows = session:execute 'SELECT key FROM system.local'
    dict:set('type', rows.type)
    dict:set('length', #rows)
    dict:set('key', rows[1].key)
}
"
--- config
    location /t {
        content_by_lua_block {
            local dict = ngx.shared.test
            ngx.say(dict:get("type"))
            ngx.say(dict:get("length"))
            ngx.say(dict:get("key"))
        }
    }
--- request
GET /t
--- response_body
ROWS
1
local
--- no_error_log
[error]



=== TEST 2: support in init_worker
--- http_config eval
"$t::Utils::HttpConfig
lua_shared_dict test 128k;
init_worker_by_lua_block {
    local dict = ngx.shared.test
    local cassandra = require 'cassandra'
    local session = cassandra.spawn_session {
        shm = 'cassandra',
        contact_points = {'127.0.0.1'}
    }

    local rows = session:execute 'SELECT key FROM system.local'
    dict:set('type', rows.type)
    dict:set('length', #rows)
    dict:set('key', rows[1].key)
}"
--- config
    location /t {
        content_by_lua_block {
            local dict = ngx.shared.test
            ngx.say(dict:get("type"))
            ngx.say(dict:get("length"))
            ngx.say(dict:get("key"))
        }
    }
--- request
GET /t
--- response_body
ROWS
1
local
--- no_error_log
[error]



=== TEST 3: support in set
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        set_by_lua_block $res {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }
            local rows = session:execute "SELECT key FROM system.local"
            return rows[1].key
        }

        echo $res;
    }
--- request
GET /t
--- response_body
local
--- no_error_log
[error]



=== TEST 4: support in rewrite
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        set $res "";
        rewrite_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }
            local rows = session:execute "SELECT key FROM system.local"
            ngx.var.res = rows[1].key
        }

        echo $res;
    }
--- request
GET /t
--- response_body
local
--- no_error_log
[error]



=== TEST 5: support in access
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        access_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }

            local rows = session:execute "SELECT key FROM system.local"
            ngx.say(rows[1].key)
        }
    }
--- request
GET /t
--- response_body
local
--- no_error_log
[error]



=== TEST 6: support in content
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }

            local rows = session:execute "SELECT key FROM system.local"
            ngx.say(rows[1].key)
        }
    }
--- request
GET /t
--- response_body
local
--- no_error_log
[error]



=== TEST 7: support in header_filter
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        return 200;

        header_filter_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }

            local rows = session:execute "SELECT key FROM system.local"
            ngx.log(ngx.ERR, "header_filter "..rows[1].key)
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[error\].*?header_filter local/



=== TEST 8: support in body_filter
--- log_level: debug
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        return 200;

        body_filter_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }

            local rows = session:execute "SELECT key FROM system.local"
            ngx.log(ngx.DEBUG, "body_filter "..rows[1].key)
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[debug\].*?body_filter local/



=== TEST 9: support in log
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        return 200;

        log_by_lua_block {
            local cassandra = require "cassandra"
            local session = cassandra.spawn_session {
                shm = "cassandra",
                contact_points = {"127.0.0.1"}
            }

            local rows = session:execute "SELECT key FROM system.local"
            ngx.log(ngx.ERR, "log "..rows[1].key)
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[error\].*?log local/



=== TEST 10: support in timer
--- http_config eval
"$t::Utils::HttpConfig"
--- config
    location /t {
        return 200;

        log_by_lua_block {
            ngx.timer.at(0, function()
                local cassandra = require "cassandra"
                local session = cassandra.spawn_session {
                    shm = "cassandra",
                    contact_points = {"127.0.0.1"}
                }

                local rows = session:execute "SELECT key FROM system.local"
                ngx.log(ngx.ERR, "timer "..rows[1].key)
            end)
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[error\].*?timer local/



=== TEST 11: luasocket fallback in non-supported contexts only
--- http_config eval
"$t::Utils::HttpConfig
lua_shared_dict test 128k;
init_by_lua_block {
    local dict = ngx.shared.test
    local cassandra = require 'cassandra'
    local socket = require 'cassandra.socket'
    local session = cassandra.spawn_session {
        shm = 'cassandra',
        contact_points = {'127.0.0.1'}
    }

    local rows = session:execute 'SELECT key FROM system.local'
    local sock = session.hosts[1].socket
    dict:set('cosocket', getmetatable(sock) ~= socket.luasocket_mt)
}
"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require 'cassandra'
            local socket = require 'cassandra.socket'
            local dict = ngx.shared.test

            local session = cassandra.spawn_session {
                shm = "cassandra"
            }

            local rows = session:execute "SELECT key FROM system.local"
            local sock = session.hosts[1].socket

            ngx.say(dict:get("cosocket"))
            ngx.say(getmetatable(sock) ~= socket.luasocket_mt)
        }
    }
--- request
GET /t
--- response_body
false
true
--- no_error_log
[error]



=== TEST 12: luasocket fallback in non-supported contexts only (bis)
--- http_config eval
"$t::Utils::HttpConfig
lua_shared_dict test 128k;
init_worker_by_lua_block {
    local dict = ngx.shared.test
    local cassandra = require 'cassandra'
    local socket = require 'cassandra.socket'
    local session = cassandra.spawn_session {
        shm = 'cassandra',
        contact_points = {'127.0.0.1'}
    }

    local rows = session:execute 'SELECT key FROM system.local'
    local sock = session.hosts[1].socket
    dict:set('cosocket', getmetatable(sock) ~= socket.luasocket_mt)
}
"
--- config
    location /t {
        access_by_lua_block {
            local cassandra = require 'cassandra'
            local socket = require 'cassandra.socket'
            local dict = ngx.shared.test

            local session = cassandra.spawn_session {
                shm = "cassandra"
            }

            local rows = session:execute "SELECT key FROM system.local"
            local sock = session.hosts[1].socket

            ngx.say(dict:get("cosocket"))
            ngx.say(getmetatable(sock) ~= socket.luasocket_mt)
        }
    }
--- request
GET /t
--- response_body
false
true
--- no_error_log
[error]
