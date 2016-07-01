#!/bin/bash

set -e

if [ "$OPENRESTY_TESTS" = true ]; then
  make prove
else
  make lint
  busted -v --coverage -o gtest --repeat 1
  luacov-coveralls -i lib/cassandra -e socket.lua
fi
