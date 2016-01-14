DEV_ROCKS=busted luacov luacov-coveralls luacheck ldoc

.PHONY: install dev clean test coverage lint doc

install:
	@luarocks make lua-cassandra-*.rockspec

dev: install
	@for rock in $(DEV_ROCKS) ; do \
		if ! command -v $$rock > /dev/null ; then \
			echo $$rock not found, installing via luarocks... ; \
			luarocks install $$rock ; \
		else \
			echo $$rock already installed, skipping ; \
		fi \
	done;

test:
	@busted -v && prove

clean:
	@rm -f luacov.*

coverage: clean
	@busted --coverage
	@luacov cassandra

lint:
	@find src spec -not -path './doc/*' -name '*.lua' | xargs luacheck -q

doc:
	@ldoc -c doc/config.ld src
