#!/bin/bash

set -e

if [ "$OPENRESTY_TESTS" != "yes" ]; then
  make lint
  busted -v --coverage -o gtest
  luacov-coveralls -i cassandra
else
  prove -l t
fi
