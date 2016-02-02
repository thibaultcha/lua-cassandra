#!/bin/bash

pip install --user PyYAML six

git clone --branch develop https://github.com/thibaultCha/ccm.git
pushd ccm
./setup.py install --user
popd
