#!/bin/bash

set -e

if [ "$OPENRESTY_TESTS" != "yes" ]; then
  busted -v --coverage -o gtest
  make lint
  luacov-coveralls -i cassandra
else
  prove -l t
fi
