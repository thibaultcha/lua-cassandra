#!/bin/bash

set -e

if [ "$OPENRESTY_TESTS" = true ]; then
  make prove
else
  export BUSTED_ARGS="-v -o gtest --repeat 1 --coverage"
  make lint
  make busted
  luacov-coveralls -i lib/cassandra -e socket.lua
fi
