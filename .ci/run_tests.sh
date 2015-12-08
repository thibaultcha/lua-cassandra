#!/bin/bash

set -e

if [ "$OPENRESTY_TESTS" != "yes" ]; then
  busted -v --coverage -o .ci/busted_print.lua
  make lint
  luacov-coveralls -i cassandra
else
  prove -l t
fi
