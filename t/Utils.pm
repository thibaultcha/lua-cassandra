package t::Utils;

use Cwd qw(cwd);

my $pwd = cwd();

our $HttpConfig = <<_EOC_;
    lua_package_path "$pwd/src/?.lua;$pwd/src/?/init.lua;;";
    lua_shared_dict cassandra 1m;
    lua_shared_dict cassandra_prepared 1m;
_EOC_

1;
