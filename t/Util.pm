package t::Util;

use strict;
use warnings;

use base 'Exporter';

my $TEST_COVERAGE_ENABLED = $ENV{TEST_COVERAGE_ENABLED};

my $LuaCovRunner = '';
if ($TEST_COVERAGE_ENABLED) {
$LuaCovRunner = <<_EOC_;
  runner = require 'luacov.runner'
  runner.tick = true
  runner.init {savestepsize = 30}
  jit.off()
_EOC_
}

our @EXPORT = qw(
    $LuaPackagePath
    $HttpConfig
);

our $LuaPackagePath = "lua_package_path './lib/?.lua;./lib/?/init.lua;;';";

our $HttpConfig = <<_EOC_;
    $LuaPackagePath
    lua_shared_dict cassandra 1m;

    init_by_lua_block {
        $LuaCovRunner
    }
_EOC_

1;
