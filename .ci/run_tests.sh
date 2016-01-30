#!/bin/bash

set -e

if [ "$OPENRESTY_TESTS" != "yes" ]; then
  make lint
  busted -v --coverage -o gtest --repeat 1
  luacov-coveralls -i cassandra
else
  prove -l t
fi
