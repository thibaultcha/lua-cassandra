#!/bin/bash

pip install --user PyYAML six

git clone https://github.com/pcmanus/ccm.git
pushd ccm
./setup.py install --user
popd
