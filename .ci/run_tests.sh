#!/bin/bash

set -e

if [ "$OPENRESTY_TESTS" != "yes" ]; then
  make lint
  busted -v --coverage -o gtest --repeat 1
  luacov-coveralls -i src/cassandra -e bit.lua -e socket.lua
else
  make prove
fi
