set -e
set -x

# -----------
# Install Lua
# -----------
pip install --user hererocks
hererocks $LUA_DIR -r^ --$LUA
export PATH=$PATH:$LUA_DIR/bin
eval `luarocks path`

# init_by_lua + plain Lua dependencies
luarocks install luasec
luarocks install luasocket

# -----------
# Install ccm
# -----------
pip install --user PyYAML six
git clone https://github.com/pcmanus/ccm.git
pushd ccm
  ./setup.py install --user
popd


if [ "$OPENRESTY_TESTS" = true ]; then
  # -----------------
  # Install OpenResty
  # -----------------
  mkdir -p $OPENRESTY_DIR

  if [ ! "$(ls -A $OPENRESTY_DIR)" ]; then
    OPENRESTY_BASE=openresty-$OPENRESTY
    curl https://openresty.org/download/$OPENRESTY_BASE.tar.gz | tar xz
    pushd $OPENRESTY_BASE
      ./configure \
        --prefix=$OPENRESTY_DIR \
        --without-http_coolkit_module \
        --without-lua_resty_dns \
        --without-lua_resty_lrucache \
        --without-lua_resty_upstream_healthcheck \
        --without-lua_resty_websocket \
        --without-lua_resty_upload \
        --without-lua_resty_string \
        --without-lua_resty_mysql \
        --without-lua_resty_redis \
        --without-http_redis_module \
        --without-http_redis2_module \
        --without-lua_redis_parser
      make
      make install
    popd
  fi

  export PATH=$PATH:$OPENRESTY_DIR/nginx/sbin

  # -------------------
  # Install Test::Nginx
  # -------------------
  curl -L https://cpanmin.us | perl - App::cpanminus
  $PERL_DIR/bin/cpanm Test::Nginx::Socket
  $PERL_DIR/bin/cpanm --local-lib=$PERL_DIR local::lib && eval $(perl -I $PERL_DIR/lib/perl5/ -Mlocal::lib)
else
  # -------------------------
  # Install test dependencies
  # -------------------------
  luarocks install busted
  luarocks install luacheck
  luarocks install luacov-coveralls
fi