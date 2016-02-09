#!/usr/bin/env bash

ccm list | grep 'lua_cassandra' | xargs -L 1 ccm remove
