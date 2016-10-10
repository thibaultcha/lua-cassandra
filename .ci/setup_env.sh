set -e

# -----------
# Install ccm
# -----------
pip install --user PyYAML six
git clone https://github.com/pcmanus/ccm.git
pushd ccm
  git checkout 18a50dfde2069e64a3124e2300ad1f0c1e08f908
  ./setup.py install --user
popd

if [ "$OPENRESTY_TESTS" = true ]; then
  #------------------------------
  # Download OpenResty + Luarocks
  #------------------------------
  OPENRESTY_DOWNLOAD=$DOWNLOAD_CACHE/openresty-$OPENRESTY
  LUAROCKS_DOWNLOAD=$DOWNLOAD_CACHE/luarocks-$LUAROCKS

  mkdir -p $OPENRESTY_DOWNLOAD $LUAROCKS_DOWNLOAD

  if [ ! "$(ls -A $OPENRESTY_DOWNLOAD)" ]; then
    pushd $DOWNLOAD_CACHE
      curl -L https://openresty.org/download/openresty-$OPENRESTY.tar.gz | tar xz
    popd
  fi

  if [ ! "$(ls -A $LUAROCKS_DOWNLOAD)" ]; then
    git clone https://github.com/keplerproject/luarocks.git $LUAROCKS_DOWNLOAD
  fi

  #-----------------------------
  # Install OpenResty + Luarocks
  #-----------------------------
  OPENRESTY_INSTALL=$INSTALL_CACHE/openresty-$OPENRESTY
  LUAROCKS_INSTALL=$INSTALL_CACHE/luarocks-$LUAROCKS

  mkdir -p $OPENRESTY_INSTALL $LUAROCKS_INSTALL

  if [ ! "$(ls -A $OPENRESTY_INSTALL)" ]; then
    pushd $OPENRESTY_DOWNLOAD
      ./configure \
        --prefix=$OPENRESTY_INSTALL \
        --without-http_coolkit_module \
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

  if [ ! "$(ls -A $LUAROCKS_INSTALL)" ]; then
    pushd $LUAROCKS_DOWNLOAD
      git checkout v$LUAROCKS
      ./configure \
        --prefix=$LUAROCKS_INSTALL \
        --lua-suffix=jit \
        --with-lua=$OPENRESTY_INSTALL/luajit \
        --with-lua-include=$OPENRESTY_INSTALL/luajit/include/luajit-2.1
      make build
      make install
    popd
  fi

  export PATH=$PATH:$OPENRESTY_INSTALL/nginx/sbin:$OPENRESTY_INSTALL/bin:$LUAROCKS_INSTALL/bin

  eval `luarocks path`

  # -------------------
  # Install Test::Nginx
  # -------------------
  curl -L https://cpanmin.us | perl - App::cpanminus
  $PERL_DIR/bin/cpanm Test::Nginx::Socket
  $PERL_DIR/bin/cpanm --local-lib=$PERL_DIR local::lib && eval $(perl -I $PERL_DIR/lib/perl5/ -Mlocal::lib)

  nginx -V
  resty -V
  prove -V
else
  # -----------
  # Install Lua
  # -----------
  LUA_DIR=$HOME/lua

  pip install --user hererocks
  hererocks $LUA_DIR -r^ --$LUA
  export PATH=$PATH:$LUA_DIR/bin
  eval `luarocks path`

  # -------------------------
  # Install test dependencies
  # -------------------------
  luarocks install busted
  luarocks install luacheck
fi

# init_by_lua + plain Lua dependencies
luarocks install luasec
luarocks install luasocket
luarocks install luacov
luarocks install luacov-coveralls

luarocks --version
