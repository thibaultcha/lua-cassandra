#!/usr/bin/env bash

set -x

which ccm
ccm stop

if [[ ! $(ccm list | grep lua_cassandra_prove) ]]; then
  if [[ -z "$SCYLLADB" ]]; then
    CASSANDRA=${1:-3.9}

    ccm create lua_cassandra_prove -v binary:$CASSANDRA -n 3
  else
    set -e
    ccm create lua_cassandra_prove --scylla --vnodes -n 3 --install-dir=$SCYLLADB_DIR
    ccm populate -n 3
    ccm start --no-wait
    exit 0
  fi
fi

ccm switch lua_cassandra_prove
ccm start --wait-for-binary-proto
