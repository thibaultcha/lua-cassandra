#!/bin/bash

if [ "$OPENRESTY_TESTS" == "yes" ]; then
  prove -l t
else
  busted -v --coverage -o .ci/busted_print.lua && make lint && luacov-coveralls -i cassandra
fi
