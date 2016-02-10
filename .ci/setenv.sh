if [ "$OPENRESTY_TESTS" == "yes" ]; then
  LUA="luajit 2.1"
fi

pip install --user hererocks
hererocks $LUA_DIR -r^ --$LUA

export PATH=$PATH:$LUA_DIR/bin:$OPENRESTY_DIR/nginx/sbin

eval `luarocks path`
