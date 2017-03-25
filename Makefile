DEV_ROCKS=busted luacov luacov-coveralls luacheck ldoc
BUSTED_ARGS ?= -v -o gtest
CASSANDRA ?= 3.10

.PHONY: install dev busted prove test clean coverage lint doc

install:
	@luarocks make

dev: install
	@for rock in $(DEV_ROCKS) ; do \
		if ! luarocks list | grep $$rock > /dev/null ; then \
			echo $$rock not found, installing via luarocks... ; \
			luarocks install $$rock ; \
		else \
			echo $$rock already installed, skipping ; \
		fi \
	done;

busted:
	@busted $(BUSTED_ARGS)

prove:
	@util/prove_ccm.sh $(CASSANDRA)
	@t/reindex t/*
	@prove

test: busted prove

clean:
	@rm -f luacov.*
	@util/clean_ccm.sh

coverage: clean
	@busted $(BUSTED_ARGS) --coverage
	@util/prove_ccm.sh $(CASSANDRA)
	@TEST_COVERAGE_ENABLED=true TEST_NGINX_TIMEOUT=30 prove
	@luacov

lint:
	@luacheck -q . \
		--std 'ngx_lua+busted' \
		--exclude-files 'docs/examples/*.lua'  \
		--no-redefined --no-unused-args

doc:
	@ldoc -c docs/config.ld lib
