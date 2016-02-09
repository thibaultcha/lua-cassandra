package t::Utils;

use Cwd qw(cwd);

my $pwd = cwd();

our $HttpConfig = <<_EOC_;
    lua_package_path "$pwd/src/?.lua;$pwd/src/?/init.lua;;";
    lua_shared_dict cassandra 1m;
    lua_shared_dict cassandra_prepared 1m;
_EOC_

our $SpawnCluster = <<_EOC_;
    init_by_lua_block {
        local cassandra = require "cassandra"
        local cluster, err = cassandra.spawn_cluster {
            shm = "cassandra",
            contact_points = {"127.0.0.1"}
        }
        if err then
            ngx.log(ngx.ERR, tostring(err))
        end
    }
_EOC_

1;
