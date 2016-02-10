export LUA_DIR=$HOME/lua
export LUAROCKS_DIR=$HOME/luarocks-$LUAROCKS

export PATH=$LUA_DIR/bin:$LUAROCKS_DIR/bin:$PATH

if [ "$OPENRESTY_TESTS" == "yes" ]; then
  export LUA=luajit-2.1
fi

bash .ci/setup_lua.sh
eval `luarocks path`

