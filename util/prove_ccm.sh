#!/usr/bin/env bash

CASSANDRA=${1:-3.9}

ccm stop
if [[ ! $(ccm list | grep lua_cassandra_prove) ]]; then
  ccm create lua_cassandra_prove -v binary:$CASSANDRA -n 3
fi
ccm switch lua_cassandra_prove
ccm start --wait-for-binary-proto
