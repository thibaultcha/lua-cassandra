#!/bin/bash

set -e

if [ "$OPENRESTY_TESTS" != "yes" ]; then
  make lint
  busted -v --coverage -o gtest --repeat 1
  luacov-coveralls -i cassandra
else
  ccm create resty_tests -v binary:$CASSANDRA -n 1
  ccm start --wait-for-binary-proto --wait-other-notice
  prove -l t
fi
