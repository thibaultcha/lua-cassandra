#!/usr/bin/env bash

CASSANDRA=${1:-2.2.4}

ccm stop
ccm create lua_cassandra_prove -v binary:$CASSANDRA -n 3
ccm switch lua_cassandra_prove
ccm start --wait-for-binary-proto
