#!/bin/bash

# A script for setting up environment for travis-ci testing.
# Sets up Lua and Luarocks.
# LUA must be "lua5.1", "lua5.2" or "luajit".
# luajit2.0 - master v2.0
# luajit2.1 - master v2.1

LUAJIT="no"

source .ci/platform.sh
cd $HOME

############
# Lua/LuaJIT
############

if [ "$PLATFORM" == "macosx" ]; then
  if [ "$LUA" == "luajit" ]; then
    LUAJIT="yes"
  fi
  if [ "$LUA" == "luajit2.0" ]; then
    LUAJIT="yes"
  fi
  if [ "$LUA" == "luajit2.1" ]; then
    LUAJIT="yes"
  fi
elif [ "$(expr substr $LUA 1 6)" == "luajit" ]; then
  LUAJIT="yes"
fi

if [ "$LUAJIT" == "yes" ]; then

  LUA_INCLUDE="$LUAJIT_DIR/include/luajit-2.0"
  mkdir -p $LUAJIT_DIR

  # If cache is empty, downlaod and compile
  if [ ! "$(ls -A $LUAJIT_DIR)" ]; then

    LUAJIT_BASE="LuaJIT-2.0.4"

    if [ "$LUA" == "luajit" ]; then
      curl http://luajit.org/download/$LUAJIT_BASE.tar.gz | tar xz
    else
      git clone http://luajit.org/git/luajit-2.0.git $LUAJIT_BASE
    fi

    pushd $LUAJIT_BASE

    if [ "$LUA" == "luajit2.1" ]; then
      git checkout v2.1
    fi

    make
    make install PREFIX=$LUAJIT_DIR
    ln -s $LUAJIT_DIR/bin/luajit $LUAJIT_DIR/bin/lua
  fi
else
  LUA_INCLUDE="$LUA_DIR/include"

  if [ "$LUA" == "lua5.1" ]; then
    curl http://www.lua.org/ftp/lua-5.1.5.tar.gz | tar xz
    mv lua-5.1.5 $LUA_DIR
  elif [ "$LUA" == "lua5.2" ]; then
    curl http://www.lua.org/ftp/lua-5.2.3.tar.gz | tar xz
    mv lua-5.2.3 $LUA_DIR
  elif [ "$LUA" == "lua5.3" ]; then
    curl http://www.lua.org/ftp/lua-5.3.0.tar.gz | tar xz
    mv lua-5.3.0 $LUA_DIR
  fi

  cd $LUA_DIR
  make $PLATFORM
  make install INSTALL_TOP=$LUA_DIR
fi

##########
# Luarocks
##########

LUAROCKS_BASE=luarocks-$LUAROCKS_VERSION
CONFIGURE_FLAGS=""

cd $HOME
curl http://luarocks.org/releases/$LUAROCKS_BASE.tar.gz | tar xz
git clone https://github.com/keplerproject/luarocks.git $LUAROCKS_BASE

mv $LUAROCKS_BASE $LUAROCKS_DIR
cd $LUAROCKS_DIR
git checkout v$LUAROCKS_VERSION

if [ "$LUAJIT" == "yes" ]; then
  LUA_DIR=$LUAJIT_DIR
elif [ "$LUA" == "lua5.1" ]; then
  CONFIGURE_FLAGS=$CONFIGURE_FLAGS" --lua-version=5.1"
elif [ "$LUA" == "lua5.2" ]; then
  CONFIGURE_FLAGS=$CONFIGURE_FLAGS" --lua-version=5.2"
elif [ "$LUA" == "lua5.3" ]; then
  CONFIGURE_FLAGS=$CONFIGURE_FLAGS" --lua-version=5.3"
fi

tree $LUA_DIR

./configure \
  --prefix=$LUAROCKS_DIR \
  --with-lua-bin=$LUA_DIR/bin \
  --with-lua-include=$LUA_INCLUDE \
  $CONFIGURE_FLAGS

make build && make install
