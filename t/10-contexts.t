use Test::Nginx::Socket::Lua;
use t::Utils;

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
    local socket = session.hosts[1].socket

    dict:set('cosocket', socket.setkeepalive ~= nil)
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
            ngx.say(dict:get("cosocket"))
        }
    }
--- request
GET /t
--- response_body
ROWS
1
local
false
--- no_error_log
[error]



=== TEST 2: suppoer in init_worker
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
    local socket = session.hosts[1].socket

    dict:set('cosocket', socket.setkeepalive ~= nil)
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
            ngx.say(dict:get("cosocket"))
        }
    }
--- request
GET /t
--- response_body
ROWS
1
local
false
--- no_error_log
[error]



=== TEST 3: suppoer in set
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
            local socket = session.hosts[1].socket

            ngx.log(ngx.DEBUG, "set "..rows[1].key.." "..tostring(socket.setkeepalive ~= nil))
            return rows[1].key
        }

        echo $res;
    }
--- request
GET /t
--- response_body
local
--- error_log eval
qr/\[debug\].*?set local false/



=== TEST 4: suppoer in rewrite
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
            local socket = session.hosts[1].socket

            ngx.var.res = rows[1].key
            ngx.log(ngx.DEBUG, "rewrite "..rows[1].key.." "..tostring(socket.setkeepalive ~= nil))
        }

        echo $res;
    }
--- request
GET /t
--- response_body
local
--- error_log eval
qr/\[debug\].*?rewrite local true/



=== TEST 5: suppoer in access
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
            local socket = session.hosts[1].socket

            ngx.say(rows[1].key)
            ngx.log(ngx.DEBUG, "access "..rows[1].key.." "..tostring(socket.setkeepalive ~= nil))
        }
    }
--- request
GET /t
--- response_body
local
--- error_log eval
qr/\[debug\].*?access local true/



=== TEST 6: suppoer in content
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
            local socket = session.hosts[1].socket

            ngx.log(ngx.DEBUG, "content "..rows[1].key.." "..tostring(socket.setkeepalive ~= nil))
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[debug\].*?content local true/



=== TEST 7: suppoer in header_filter
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
            local socket = session.hosts[1].socket

            ngx.log(ngx.DEBUG, "header_filter "..rows[1].key.." "..tostring(socket.setkeepalive ~= nil))
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[debug\].*?header_filter local false/



=== TEST 8: suppoer in body_filter
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
            local socket = session.hosts[1].socket

            ngx.log(ngx.DEBUG, "body_filter "..rows[1].key.." "..tostring(socket.setkeepalive ~= nil))
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[debug\].*?body_filter local false/



=== TEST 9: suppoer in log
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
            local socket = session.hosts[1].socket

            ngx.log(ngx.DEBUG, "log "..rows[1].key.." "..tostring(socket.setkeepalive ~= nil))
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[debug\].*?log local false/



=== TEST 10: suppoer in timer
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
                local socket = session.hosts[1].socket

                ngx.log(ngx.DEBUG, "timer "..rows[1].key.." "..tostring(socket.setkeepalive ~= nil))
            end)
        }
    }
--- request
GET /t
--- response_body

--- error_log eval
qr/\[debug\].*?timer local true/



=== TEST 11: luasocket fallback in non-supported contexts only
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
    local socket = session.hosts[1].socket
    dict:set('cosocket', socket.setkeepalive ~= nil)
}
"
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require 'cassandra'
            local dict = ngx.shared.test

            local session = cassandra.spawn_session {
                shm = "cassandra"
            }

            local rows = session:execute "SELECT key FROM system.local"
            local socket = session.hosts[1].socket

            ngx.say(dict:get("cosocket"))
            ngx.say(socket.setkeepalive ~= nil)
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
    local session = cassandra.spawn_session {
        shm = 'cassandra',
        contact_points = {'127.0.0.1'}
    }

    local rows = session:execute 'SELECT key FROM system.local'
    local socket = session.hosts[1].socket
    dict:set('cosocket', socket.setkeepalive ~= nil)
}
"
--- config
    location /t {
        access_by_lua_block {
            local cassandra = require 'cassandra'
            local dict = ngx.shared.test

            local session = cassandra.spawn_session {
                shm = "cassandra"
            }

            local rows = session:execute "SELECT key FROM system.local"
            local socket = session.hosts[1].socket

            ngx.say(dict:get("cosocket"))
            ngx.say(socket.setkeepalive ~= nil)
        }
    }
--- request
GET /t
--- response_body
false
true
--- no_error_log
[error]
