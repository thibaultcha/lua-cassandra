# lua-cassandra

This library is a pure Lua implementation of the Cassandra CQL binary protocol. It is a fork of [jbochi/lua-resty-cassandra][lua-resty-cassandra] with support for more features, a few bug fixes and a more complete documentation.

Among the features it adds, this fork supports both binary protocols v2 and v3.

The following table describes which version(s) of the binary protocol is supported for each version of Cassandra:

<br />
<table class="module_list">
  <tr><td>Cassandra Version</td><td>Binary Protocol support</td></tr>
  <tr><td>1.2</td><td>1</td></tr>
  <tr><td>2.0</td><td>1, 2</td></tr>
  <tr><td>2.1</td><td>1, 2, 3</td></tr>
  <tr><td>2.2</td><td>1, 2, 3</td></tr>
</table>
<br />

Since lua-cassandra supports binary protocol v2 and v3, it only supports **Cassandra 2.0 and later**.

## Installation

#### Luarocks

Installation through [luarocks][luarocks-url] is recommended:

```bash
$ luarocks install lua-cassandra
```

#### Manual

Simply copy the `src/` folder in your application.

## Usage

- To use the binary protocol v3 (Cassandra 2.1 and later):

```lua
local cassandra = require "cassandra"
```

- To use the binary protocol v2 (Legacy support for Cassandra 2.0):

```lua
local cassandra = require "cassandra.v2"
```

See the `cassandra` module for a detailed list of available functions.

Once you have an instance of `cassandra`, use it to create sessions. See the `session` module for a detailed list of functions.

Finally, check the examples section for concrete examples of basic or advanced usage.

[luarocks-url]: https://luarocks.org
[lua-resty-cassandra]: https://github.com/jbochi/lua-resty-cassandra
