# vim:set ts=4 sw=4 et fdm=marker:
use Test::Nginx::Socket::Lua;
use t::Util;

our $HttpConfig = $t::Util::HttpConfig;

plan tests => repeat_each() * blocks() * 3;

run_tests();

__DATA__

=== TEST 1: Cluster module fields
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            ngx.say(Cluster._VERSION)
        }
    }
--- request
GET /t
--- response_body_like
[0-9]\.[0-9]\.[0-9]
--- no_error_log
[error]



=== TEST 2: cluster.new() invalid opts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new('')
            if not cluster then
                ngx.say(err)
            end

            cluster, err = Cluster.new({shm = true})
            if not cluster then
                ngx.say(err)
            end

            cluster, err = Cluster.new({shm = 'invalid_shm'})
            if not cluster then
                ngx.say(err)
            end

            cluster, err = Cluster.new({keyspace = 123})
            if not cluster then
                ngx.say(err)
            end

            cluster, err = Cluster.new({timeout_read = 'foo'})
            if not cluster then
                ngx.say(err)
            end

            cluster, err = Cluster.new({timeout_connect = 'foo'})
            if not cluster then
                ngx.say(err)
            end
        }
    }
--- request
GET /t
--- response_body
opts must be a table
shm must be a string
no shared dict invalid_shm
keyspace must be a string
timeout_read must be a number
timeout_connect must be a number
--- no_error_log
[error]



=== TEST 3: cluster.new() default opts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end
        }
    }
--- request
GET /t
--- response_body

--- no_error_log
[error]



=== TEST 4: cluster.new() retry_on_timeout opts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end
            ngx.say('default: ', cluster.retry_on_timeout)

            cluster, err = Cluster.new({retry_on_timeout = false})
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end
            ngx.say('false opt: ', cluster.retry_on_timeout)

            cluster, err = Cluster.new {retry_on_timeout = true}
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end
            ngx.say('true opt: ', cluster.retry_on_timeout)
        }
    }
--- request
GET /t
--- response_body
default: true
false opt: false
true opt: true
--- no_error_log
[error]



=== TEST 5: cluster.new() peers opts and keyspace
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local cassandra = require 'cassandra'
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                keyspace = 'system',
                ssl = true,
                verify = true,
                auth = cassandra.auth_providers.plain_text('cassandra', 'cassandra')
            }
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say('keyspace: ', cluster.keyspace)
            ngx.say('ssl: ', cluster.peers_opts.ssl)
            ngx.say('verify: ', cluster.peers_opts.verify)
            ngx.say('auth: ', type(cluster.peers_opts.auth))
        }
    }
--- request
GET /t
--- response_body
keyspace: system
ssl: true
verify: true
auth: table
--- no_error_log
[error]



=== TEST 6: cluster.refresh() with invalid contact_points
--- http_config eval
qq {
    $::HttpConfig
    lua_socket_log_errors off;
}
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                contact_points = {'255.255.255.254'},
                timeout_connect = 10
            }
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err = cluster:refresh()
            ngx.say(ok)
            ngx.say(err)
        }
    }
--- request
GET /t
--- response_body
nil
all hosts tried for query failed. 255.255.255.254: host seems unhealthy, considering it down (timeout)
--- no_error_log
[error]



=== TEST 7: cluster.refresh() sets hosts in shm
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, 'could not spawn cluster: ', err)
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, 'could not refresh: ', err)
            end

            local shm = ngx.shared.cassandra
            local keys = shm:get_keys()
            assert(#keys > 0)

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, 'could not get shm peers: ', err)
            end

            for i = 1, #peers do
                local p = peers[i]
                ngx.say(p.host..' '..p.unhealthy_at..' '..p.reconn_delay, ' ', p.up)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.3 0 0 true
127.0.0.2 0 0 true
127.0.0.1 0 0 true
--- no_error_log
[error]



=== TEST 8: cluster.refresh() sets hosts in a custom shm
--- http_config eval
qq{
    $::HttpConfig
    lua_shared_dict custom 1m;
}
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                shm = 'custom'
            }
            if not cluster then
                ngx.log(ngx.ERR, 'could not spawn cluster: ', err)
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, 'could not refresh: ', err)
            end

            local shm = ngx.shared.custom
            local keys = shm:get_keys()
            assert(#keys > 0)

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, 'could not get shm peers: ', err)
            end

            for i = 1, #peers do
                local p = peers[i]
                ngx.say(p.host..' '..p.unhealthy_at..' '..p.reconn_delay, ' ', p.up)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.3 0 0 true
127.0.0.2 0 0 true
127.0.0.1 0 0 true
--- no_error_log
[error]



=== TEST 9: cluster.refresh() sets data_center/release_version of each host
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, 'could not spawn cluster: ', err)
                return
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, 'could not refresh: ', err)
                return
            end

            local shm = ngx.shared.cassandra
            local keys = shm:get_keys()
            assert(#keys > 0)

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, 'could not get shm peers: ', err)
                return
            end

            for i = 1, #peers do
                local p = peers[i]
                ngx.say(p.host, ' ', p.data_center, ' ', p.release_version)
            end
        }
    }
--- request
GET /t
--- response_body_like
\d+\.\d+\.\d+\.\d+.*?\S+.*?\d+\.\d+\.?\d*
\d+\.\d+\.\d+\.\d+.*?\S+.*?\d+\.\d+\.?\d*
\d+\.\d+\.\d+\.\d+.*?\S+.*?\d+\.\d+\.?\d*
--- no_error_log
[error]



=== TEST 10: cluster.refresh() sets protocol_version
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, 'could not spawn cluster: ', err)
                return
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, 'could not refresh: ', err)
                return
            end

            local protocol_version, err = ngx.shared.cassandra:get('protocol:version:')
            if err then
                ngx.log(ngx.ERR, 'could not get protocol_version: ', err)
                return
            end

            ngx.say('protocol_version: ', protocol_version)
        }
    }
--- request
GET /t
--- response_body_like
protocol_version: \d
--- no_error_log
[error]



=== TEST 11: cluster.refresh() inits cluster
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, 'could not spawn cluster: ', err)
            end

            if cluster.init then
                ngx.log(ngx.ERR, 'cluster already init')
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, 'could not refresh: ', err)
            end

            ngx.say('init: ', cluster.init)
        }
    }
--- request
GET /t
--- response_body
init: true
--- no_error_log
[error]



=== TEST 12: cluster.refresh() removes old peers details/status
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            -- insert fake peers
            cluster:set_peer('127.0.0.253', true, 0, 0, 'foocenter1', '0.0')
            cluster:set_peer('127.0.0.254', true, 0, 0, 'foocenter1', '0.0')

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, err)
                return
            end

            for i = 1, #peers do
                local p = peers[i]
                ngx.say(p.host, ' ', p.up)
            end

            ngx.say('status: ', cluster.shm:get('127.0.0.253'))
            ngx.say('status: ', cluster.shm:get('127.0.0.254'))

            local _, err = cluster:get_peer('127.0.0.253')
            ngx.say('info: ', err)
            local _, err = cluster:get_peer('127.0.0.254')
            ngx.say('info: ', err)
        }
    }
--- request
GET /t
--- response_body
127.0.0.3 true
127.0.0.2 true
127.0.0.1 true
status: nil
status: nil
info: no host details for 127.0.0.253
info: no host details for 127.0.0.254
--- no_error_log
[error]



=== TEST 13: cluster.refresh() does not alter existing peers records and status
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
            end

            -- insert previous peers with some infos
            cluster:set_peer('127.0.0.1', false, 1000, 1461030739000, '', '')

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, err)
                return
            end

            for i = 1, #peers do
                ngx.say(peers[i].host)
            end

            local peer_rec, err = cluster:get_peer('127.0.0.1')
            if not peer_rec then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say('infos: ', peer_rec.reconn_delay, ' ', peer_rec.unhealthy_at)
            ngx.say('up: ', peer_rec.up)
        }
    }
--- request
GET /t
--- response_body
127.0.0.3
127.0.0.2
127.0.0.1
infos: 1000 1461030739000
up: false
--- no_error_log
[error]



=== TEST 14: get_peers() corrupted shm
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
            end

            cluster.shm:set('host:rec:127.0.0.1', false)

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, err)
            end
        }
    }
--- request
GET /t
--- response_body

--- error_log
corrupted shm



=== TEST 15: get_peers() returns nil if no peers
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local peers, err = cluster:get_peers()
            if err then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say('is nil: ', peers == nil)
        }
    }
--- request
GET /t
--- response_body
is nil: true
--- no_error_log
[error]



=== TEST 16: set_peer_down()/set_peer_up()/can_try_peer() set shm booleans for nodes status
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
            end

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, err)
            end

            for i = 1, #peers do
                ok = cluster:can_try_peer(peers[i].host)
                ngx.say(peers[i].host, ' default: ', ok)

                ok, err = cluster:set_peer_down(peers[i].host)
                if not ok then
                    ngx.log(ngx.ERR, err)
                    return
                end

                ok = cluster:can_try_peer(peers[i].host)
                ngx.say(peers[i].host, ' after down: ', ok)

                ok, err = cluster:set_peer_up(peers[i].host)
                if not ok then
                    ngx.log(ngx.ERR, err)
                    return
                end

                ok = cluster:can_try_peer(peers[i].host)
                ngx.say(peers[i].host, ' after up: ', ok)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.3 default: true
127.0.0.3 after down: false
127.0.0.3 after up: true
127.0.0.2 default: true
127.0.0.2 after down: false
127.0.0.2 after up: true
127.0.0.1 default: true
127.0.0.1 after down: false
127.0.0.1 after up: true
--- no_error_log
[error]



=== TEST 17: set_peer_down()/set_peer_up() use existing host details if exists
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err = cluster:refresh()
            if not ok then
                 ngx.log(ngx.ERR, err)
                 return
            end

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, err)
                return
            end

            for i = 1, #peers do
                ok, err = cluster:set_peer_down(peers[i].host)
                if not ok then
                    ngx.log(ngx.ERR, err)
                    return
                end

                local peer, err = cluster:get_peer(peers[i].host)
                if not peer then
                    ngx.log(ngx.ERR, err)
                    return
                end

                ngx.say(peer.host, ' after down: ', peer.data_center, ' ', peer.release_version)

                ok, err = cluster:set_peer_up(peers[i].host)
                if not ok then
                    ngx.log(ngx.ERR, err)
                    return
                end

                peer, err = cluster:get_peer(peers[i].host)
                if not peer then
                    ngx.log(ngx.ERR, err)
                    return
                end

                ngx.say(peer.host, ' after up: ', peer.data_center, ' ', peer.release_version)
            end
        }
    }
--- request
GET /t
--- response_body_like
127\.0\.0\.3 after down: datacenter1 \d+\.\d+\.?\d*
127\.0\.0\.3 after up: datacenter1 \d+\.\d+\.?\d*
127\.0\.0\.2 after down: datacenter1 \d+\.\d+\.?\d*
127\.0\.0\.2 after up: datacenter1 \d+\.\d+\.?\d*
127\.0\.0\.1 after down: datacenter1 \d+\.\d+\.?\d*
127\.0\.0\.1 after up: datacenter1 \d+\.\d+\.?\d*
--- no_error_log
[error]



=== TEST 18: set_peer_down()/set_peer_up() defaults hosts details if not exists
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local fixtures = {'127.0.0.253', '127.0.0.254'}

            for i = 1, #fixtures do
                ok, err = cluster:set_peer_down(fixtures[i])
                if not ok then
                    ngx.log(ngx.ERR, err)
                    return
                end

                local peer, err = cluster:get_peer(fixtures[i])
                if not peer then
                    ngx.log(ngx.ERR, err)
                    return
                end

                ngx.say(peer.host, ' after down: ', peer.up, ' ', peer.data_center,
                        ' ', peer.release_version)

                ok, err = cluster:set_peer_up(fixtures[i])
                if not ok then
                    ngx.log(ngx.ERR, err)
                    return
                end

                peer, err = cluster:get_peer(fixtures[i])
                if not peer then
                    ngx.log(ngx.ERR, err)
                    return
                end

                ngx.say(peer.host, ' after up: ', peer.up, ' ', peer.data_center,
                        ' ', peer.release_version)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.253 after down: false nil nil
127.0.0.253 after up: true nil nil
127.0.0.254 after down: false nil nil
127.0.0.254 after up: true nil nil
--- no_error_log
[error]



=== TEST 19: set_peer_down()/set_peer_up() use reconnection policy (update peer_rec delays)
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
         local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local delay = cluster.reconn_policy:next_delay('foo')

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            ok, err = cluster:set_peer_down('127.0.0.1')
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            local peer_rec, err = cluster:get_peer('127.0.0.1')
            if not peer_rec then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say('down')
            ngx.say('unhealthy_at: ', peer_rec.unhealthy_at > 0)
            ngx.say('reconn_delay: ', peer_rec.reconn_delay == delay)

            local ok, err = cluster:set_peer_up('127.0.0.1')
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            peer_rec, err = cluster:get_peer('127.0.0.1')
            if not peer_rec then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say('up')
            ngx.say('unhealthy_at: ', peer_rec.unhealthy_at)
            ngx.say('reconn_delay: ', peer_rec.reconn_delay)

            ok, err = cluster:set_peer_down('127.0.0.1')
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            peer_rec, err = cluster:get_peer('127.0.0.1')
            if not peer_rec then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say('down again')
            ngx.say('unhealthy_at: ', peer_rec.unhealthy_at > 0)
            ngx.say('reconn_delay: ', peer_rec.reconn_delay == delay)
        }
    }
--- request
GET /t
--- response_body
down
unhealthy_at: true
reconn_delay: true
up
unhealthy_at: 0
reconn_delay: 0
down again
unhealthy_at: true
reconn_delay: true
--- no_error_log
[error]



=== TEST 20: can_try_peer() use reconnection policy to decide when node is down
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err, is_retry = cluster:can_try_peer('127.0.0.1')
            if err then
                ngx.log(ngx.ERR, err)
                return
            end
            ngx.say('before down: ', ok, ' ', is_retry)

            ok, err = cluster:set_peer_down('127.0.0.1')
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            ok, err, is_retry = cluster:can_try_peer('127.0.0.1')
            if err then
                ngx.log(ngx.ERR, err)
                return
            end
            ngx.say('until delay: ', ok, ' ', is_retry)

            -- still down but speed up reconnection delay
            ok, err = cluster:set_peer('127.0.0.1', false, 1000, 1460780710809, '', '')
            if not ok then
                ngx.log(ngx.ERR, 'could not set peer_rec: ', err)
                return
            end

            ok, err, is_retry = cluster:can_try_peer('127.0.0.1')
            if err then
                ngx.log(ngx.ERR, err)
                return
            end
            ngx.say('after delay: ', ok, ' ', is_retry)
        }
    }
--- request
GET /t
--- response_body
before down: true nil
until delay: false true
after delay: true true
--- no_error_log
[error]



=== TEST 21: next_coordinator() uses load balancing policy
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
            end

            local coordinator = cluster:next_coordinator()
            ngx.say('coordinator 1: ', coordinator.host)

            coordinator = cluster:next_coordinator()
            ngx.say('coordinator 2: ', coordinator.host)

            coordinator = cluster:next_coordinator()
            ngx.say('coordinator 3: ', coordinator.host)
        }
    }
--- request
GET /t
--- response_body
coordinator 1: 127.0.0.3
coordinator 2: 127.0.0.2
coordinator 3: 127.0.0.1
--- no_error_log
[error]



=== TEST 22: next_coordinator() returns no host available errors
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
            end

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, err)
            end

            for i = 1, #peers do
                local ok, err = cluster:set_peer_down(peers[i].host)
                if not ok then
                    ngx.log(ngx.ERR, err)
                    return
                end
            end

            local coordinator, err = cluster:next_coordinator()
            if not coordinator then
                ngx.say(err)
            end
        }
    }
--- request
GET /t
--- response_body
all hosts tried for query failed. 127.0.0.2: host still considered down. 127.0.0.3: host still considered down. 127.0.0.1: host still considered down
--- no_error_log
[error]



=== TEST 23: next_coordinator() avoids down hosts
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
            end

            local ok, err = cluster:set_peer_down('127.0.0.1')
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            for i = 1, 5 do
                local coordinator, err = cluster:next_coordinator()
                if not coordinator then
                    ngx.say(err)
                end
                ngx.say(i, ' ', coordinator.host)
            end
        }
    }
--- request
GET /t
--- response_body
1 127.0.0.3
2 127.0.0.2
3 127.0.0.3
4 127.0.0.3
5 127.0.0.2
--- no_error_log
[error]



=== TEST 24: next_coordinator() marks nodes as down
--- http_config eval
qq {
    lua_socket_log_errors off;
    $::HttpConfig
}
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new {
                timeout_connect = 100
            }
            if not cluster then
                ngx.log(ngx.ERR, err)
            end

            -- insert fake nodes
            local ok, err = cluster:set_peer_up('255.255.255.254')
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end
            ok, err = cluster:set_peer_up('255.255.255.253')
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, err)
                return
            end

            -- init cluster as if it was refreshed
            cluster.lb_policy:init(peers)
            cluster.init = true

            -- attempt to get next coordinator
            local coordinator, err = cluster:next_coordinator()
            if not coordinator then
                ngx.say('all down: ', err)
            end

            -- verify they were marked down
            for i = 1, #peers do
                local ok, err = cluster:can_try_peer(peers[i].host)
                if err then
                    ngx.log(ngx.ERR, err)
                    return
                end

                ngx.say('can try peer ', peers[i].host, ': ', ok)
            end
        }
    }
--- request
GET /t
--- response_body
all down: all hosts tried for query failed. 255.255.255.254: host seems unhealthy, considering it down (timeout). 255.255.255.253: host seems unhealthy, considering it down (timeout)
can try peer 255.255.255.254: false
can try peer 255.255.255.253: false
--- no_error_log
[error]



=== TEST 25: next_coordinator() retries down host as per reconnection policy and ups them back
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            local peers, err = cluster:get_peers()
            if not peers then
                ngx.log(ngx.ERR, err)
                return
            end

            -- mark all nodes as down
            for i = 1, #peers do
                local ok, err = cluster:set_peer_down(peers[i].host)
                if not ok then
                    ngx.log(ngx.ERR, err)
                    return
                end

                -- still down, but simulate delay for retry from reconnection policy
                -- reconn_delay: 1000
                -- unhealthy_at: 1460780710809 (past)
                ok, err = cluster:set_peer(peers[i].host, false, 1000, 1460780710809)
                if not ok then
                    ngx.log(ngx.ERR, 'could not set peer_rec: ', err)
                    return
                end
            end

            -- try to get some coordinators
            -- since the delay is passed, they should be marked back 'up'
            -- because of this call
            for i = 1, #peers do
                local coordinator, err = cluster:next_coordinator()
                if not coordinator then
                    ngx.log(ngx.ERR, 'could not get coordinator: ', err)
                    return
                end
            end

            -- they should all be up by now
            for i = 1, #peers do
                local ok, err = cluster:can_try_peer(peers[i].host)
                if err then
                    ngx.log(ngx.ERR, 'error in can_try_peer ', peers[i].host..': ', err)
                    return
                end
                ngx.say(peers[i].host, ' is back up: ', ok)
            end
        }
    }
--- request
GET /t
--- response_body
127.0.0.3 is back up: true
127.0.0.2 is back up: true
127.0.0.1 is back up: true
--- no_error_log
[error]



=== TEST 26: next_coordinator() sets coordinator keyspace on connect
--- http_config eval: $::HttpConfig
--- config
    location /t {
        content_by_lua_block {
            local Cluster = require 'resty.cassandra.cluster'
            local cluster, err = Cluster.new()
            if not cluster then
                ngx.log(ngx.ERR, err)
                return
            end

            local ok, err = cluster:refresh()
            if not ok then
                ngx.log(ngx.ERR, err)
                return
            end

            local coordinator, err = cluster:next_coordinator({
                keyspace = 'system'
            })
            if not coordinator then
                ngx.log(ngx.ERR, err)
                return
            end

            ngx.say(coordinator.keyspace)
        }
    }
--- request
GET /t
--- response_body
system
--- no_error_log
[error]
