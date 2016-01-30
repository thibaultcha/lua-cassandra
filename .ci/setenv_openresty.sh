export OPENRESTY_DIR=$HOME/openresty-$OPENRESTY
export PATH=$OPENRESTY_DIR/nginx/sbin:$PATH

bash .ci/setup_openresty.sh
