#!/bin/bash

set -e

if [ "$OPENRESTY_TESTS" = true ]; then
  export TEST_COVERAGE_ENABLED=1
  export TEST_NGINX_TIMEOUT=30
  make prove
else
  export BUSTED_ARGS="-v -o gtest --repeat 1 --coverage"
  make lint
  make busted
fi

luacov-coveralls
