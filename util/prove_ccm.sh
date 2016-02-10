#!/usr/bin/env bash

CASSANDRA=${1:-2.2.4}
echo $CASSANDRA
ccm stop
ccm create resty_tests -v binary:$CASSANDRA -n 1
ccm start --wait-for-binary-proto
