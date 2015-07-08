DEV_ROCKS=busted luacov luacov-coveralls luacheck ldoc

.PHONY: dev clean test coverage lint

dev:
	@for rock in $(DEV_ROCKS) ; do \
		if ! command -v $$rock &> /dev/null ; then \
			echo $$rock not found, installing via luarocks... ; \
			luarocks install $$rock ; \
		else \
			echo $$rock already installed, skipping ; \
		fi \
	done;

test:
	@busted -v

clean:
	@rm -f luacov.*

coverage: clean
	@busted --coverage
	@luacov cassandra

lint:
	@find . -not -path './doc/*' -name '*.lua' | xargs luacheck -q
