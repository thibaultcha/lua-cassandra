DEV_ROCKS=busted luacov luasec luacov-coveralls luacheck ldoc

.PHONY: install dev test prove clean coverage lint doc

install:
	@luarocks make lua-cassandra-*.rockspec

dev: install
	@for rock in $(DEV_ROCKS); do\
		if ! command -v $$rock > /dev/null; then\
			echo $$rock not found, installing via luarocks...;\
			luarocks install $$rock;\
		else\
			echo $$rock already installed, skipping;\
		fi\
	done;

test:
	@busted -v -o gtest

prove:
	@util/reindex t/* && prove

clean:
	@rm -f luacov.*
	@util/clean_ccm.sh

coverage: clean
	@busted -v --coverage
	@luacov cassandra

lint:
	@find src spec -not -path './doc/*' -name '*.lua' | xargs luacheck -q

doc:
	@ldoc -c doc/config.ld src
