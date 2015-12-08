#!/bin/bash

set -e

if [ "$OPENRESTY_TESTS" != "yes" ]; then
  exit
fi

if [ ! "$(ls -A $OPENRESTY_DIR)" ]; then
  OPENRESTY_BASE=ngx_openresty-$OPENRESTY_VERSION

  curl https://openresty.org/download/$OPENRESTY_BASE.tar.gz | tar xz
  pushd $OPENRESTY_BASE
  ./configure \
    --prefix=$OPENRESTY_DIR
  make
  make install
  popd $OPENRESTY_BASE
fi

echo "ls $OPENRESTY_DIR"
ls $OPENRESTY_DIR

cpan install Test::Nginx::Socket
