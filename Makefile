DEV_ROCKS=busted luacov-coveralls luacheck ldoc

.PHONY: install dev busted prove test clean coverage lint doc

install:
	@luarocks make

dev: install
	@for rock in $(DEV_ROCKS); do\
		if ! command -v $$rock > /dev/null; then\
			echo $$rock not found, installing via luarocks...;\
			luarocks install $$rock;\
		else\
			echo $$rock already installed, skipping;\
		fi\
	done;

busted:
	@busted -v -o gtest

prove:
	@util/prove_ccm.sh
	@t/reindex t/*
	@prove

test: busted prove

clean:
	@rm -f luacov.*
	@util/clean_ccm.sh

coverage: clean
	@busted -v -o gtest --coverage
	@luacov cassandra

lint:
	@luacheck -q . \
		--std 'ngx_lua+busted' \
		--exclude-files 'doc/examples/*.lua'  \
		--no-redefined --no-unused-args

doc:
	@ldoc -c doc/config.ld lib
