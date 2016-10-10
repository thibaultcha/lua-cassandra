#!/usr/bin/env bash

ccm stop
ccm remove
ccm list | grep 'lua_cassandra' | xargs -L 1 ccm remove
rm -rf $HOME/.ccm/lua_cassandra*
ccm list
