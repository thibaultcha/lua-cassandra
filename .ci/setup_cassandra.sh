#!/bin/bash

HOSTS=${CASSANDRA_HOSTS:-127.0.0.1}
ARR=(${HOSTS//,/ })

pip install --user PyYAML six
git clone https://github.com/pcmanus/ccm.git
pushd ccm
./setup.py install --user
popd
ccm create test -v binary:$CASSANDRA -n ${#ARR[@]} -d
ccm start -v
ccm status
