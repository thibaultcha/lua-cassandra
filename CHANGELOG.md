### [1.5.2]

> Released on: 2022/05/20

#### Fixed

- Improve DC-aware LB policies robustness.
  [#147](https://github.com/thibaultcha/lua-cassandra/pull/147)
- Ensure request-aware + DC-aware LB policy prioritizes local peers over remote
  ones.

### [1.5.1]

> Released on: 2021/02/18

##### Added

- Enable TLS 1.2 in LuaSec fallback and disable SSLv2, SSLv3, and TLS 1.0.
  [#138](https://github.com/thibaultcha/lua-cassandra/pull/138)
- Enable LuaSec fallback in ngx_stream_lua's `preread` phase.
  [#136](https://github.com/thibaultcha/lua-cassandra/pull/136)

#### Fixed

- Ensure `cluster:refresh()` always releases its lock when encountering an
  error.
  [#140](https://github.com/thibaultcha/lua-cassandra/pull/140)

### [1.5.0]

> Released on: 2019/09/25

##### Added

- The `cluster:refresh()` method is now safe to be called at runtime,
  allowing cluster topology changes (added or removed nodes) to be taken into
  consideration.
  [#134](https://github.com/thibaultcha/lua-cassandra/pull/134)
- The `cluster:refresh()` method now accepts an optional `timeout` argument,
  and returns the topology changes (if any).
  [#134](https://github.com/thibaultcha/lua-cassandra/pull/134)

### [1.4.0]

> Released on: 2019/05/09

##### Changed

- Improve `host still considered down` error logs to include the error causing
  each node to be considered as "DOWN", as well as the duration for which it
  will still be considered "DOWN".
  [#129](https://github.com/thibaultcha/lua-cassandra/pull/129)

### [1.3.4]

> Released on: 2019/01/23

##### Changed

- Do not log the "rpc_address set to 0.0.0.0" warning when `opts.silent` is on.
  [#126](https://github.com/thibaultcha/lua-cassandra/pull/126)

### [1.3.3]

> Released on: 2018/11/09

##### Added

- Support for binary CQL frames with custom payload.
  [#119](https://github.com/thibaultcha/lua-cassandra/pull/119)

##### Fixed

- Favor the `rpc_address` value to connect to the provided contact
  points.
  [#122](https://github.com/thibaultcha/lua-cassandra/pull/122)

### [1.3.2]

> Released on: 2018/08/10

##### Fixed

- Environments with DNS load-balancing in effect for `contact_points` provided
  as hostnames (e.g. Kubernetes with `contact_points = { "cassandra" }`) could
  result in `no host details for <peer IP>` errors when using multiple
  instances of the Cluster module. This is now fixed.
  [#118](https://github.com/thibaultcha/lua-cassandra/pull/118)

### [1.3.1] - 2018/07/02

##### Fixed

- The new request-aware load-balancing policies can now be used in the
  init_by_lua* context when using the lua-resty-core module (only applies
  to OpenResty environments).
  [#117](https://github.com/thibaultcha/lua-cassandra/pull/117)

### [1.3.0] - 2018/06/14

##### Added

- New request-aware load-balancing policy for OpenResty environments.
- New request-aware + datacenter-aware load-balancing policy for OpenResty
  environments.
  Thanks [@kikito](https://github.com/kikito) for the patch!
  [#114](https://github.com/thibaultcha/lua-cassandra/pull/114)

### [1.2.3] - 2017/07/20

##### Added

- Expose the `check_schema_consensus()` method of the Cluster module for
  host applications to use.

##### Fixed

- Safely share peers across workers in the Cluster module.
  [#97](https://github.com/thibaultcha/lua-cassandra/pull/97)

### [1.2.2] - 2017/05/17

##### Added

- New `cafile` option for the Cluster module. This allows supporting SSL
  connections to Cassandra clusters when lua-cassandra is used in contexts
  that do not support cosockets, and fallback on LuaSocket.
  [#95](https://github.com/thibaultcha/lua-cassandra/pull/95)

### [1.2.1] - 2017/04/03

##### Fixed

- Force the Nginx time to be updated when checking for schema consensus
  timeout.
  [#90](https://github.com/thibaultcha/lua-cassandra/pull/90)

### [1.2.0] - 2017/03/24

##### Added

- Methods to manually add and remove peers from a Cluster module instance.
  [#87](https://github.com/thibaultcha/lua-cassandra/pull/87)

### [1.1.1] - 2016/02/28

##### Added

- Expose the underlying `first_coordinator` and `wait_schema_consensus`
functions from the Cluster module.

### [1.1.0] - 2016/01/12

##### Changed

- :warning: Peers are now part of different connection pools depending on their
keyspace. This can fix eventual issues when using several keyspaces with a
single peer/cluster instance.
[6c0db5e](https://github.com/thibaultcha/lua-cassandra/commit/6c0db5e178daa119c6df2b40ff648349cba50799)
This is a breaking change:
  ```lua
    -- before:
    local peer = cassandra.new()
    peer:connect()
    peer:set_keyspace('my_keyspace')

    -- after:
    local peer = cassandra.new()
    peer:connect()
    peer:change_keyspace('my_keyspace') -- closes the underlying connection and open a new one
  ```

##### Added

- New `coordinator_options` for `execute()`/`batch()`/`iterate()` allowing for
more granularity in keyspace settings. Accepted options are `keyspace` and
`no_keyspace`. Example:
  ```lua
    local Cluster = cluster.new {
      keyspace = 'my_keyspace'
    }

    local res = cluster:execute('SELECT * FROM local', nil, {
      keyspace = 'system' -- will spawn or reuse a peer with 'system' keyspace
      --no_keyspace = true -- would disable setting a keyspace for this request
    })
  ```
[cdc6607](https://github.com/thibaultcha/lua-cassandra/commit/cdc6607d26d23d6d9e1268d3db316aaf90ce51a8)
- Support for binary protocol v4.
[#61](https://github.com/thibaultcha/lua-cassandra/pull/61)
  - New `cassandra.null` CQL marshalling type. This type is different than
  `cassandra.unset` for protocol v4 and will set to **null** existing columns
  (in protocol v4 usage only).
  - Parse `SCHEMA_CHANGE` results for `FUNCTION` and `AGGREGATE`.
  - The Cluster module now parses warnings contained in response frames and
  logs them at the `ngx.WARN` level.
- Implement a `silent` option for `Cluster.new()` to disable logging in the
  nginx error logs.
  [#60](https://github.com/thibaultcha/lua-cassandra/pull/69)
- Implement a `lock_timeout` option for `Cluster.new()` to specify a max
  waiting time in seconds for the cluster refreshing and requests preparing
  mutexes. This option prevents such mutexes to hang for too long.
  [2bd3d66](https://github.com/thibaultcha/lua-cassandra/commit/2bd3d66eb26530490391ffb0f5dc366cc9fd0874)
- The `cluster:refresh()` method now returns the list of fetched Cassandra
  nodes from the cluster as a third return value.
  [34f5f11](https://github.com/thibaultcha/lua-cassandra/commit/34f5f1168f5a69dddf53c5564b8577250a7fde0a)

##### Fixed

- Correctly logs the address of peers being set UP or DOWN in the warning logs.
  [40fd870](https://github.com/thibaultcha/lua-cassandra/commit/40fd8705b55059e55e6687394354937d2dead2c2)
- Better error messages for SSL handshake/locking failures.
- Better handling in case the shm containing the cluster info is full. We do
  not override previous values at the risk of losing cluster nodes info, but
  error out with the `"no memory"` error instead.
  [4520a3b](https://github.com/thibaultcha/lua-cassandra/commit/4520a3b034a7b4d9d00975c95ecf834ce263f048)
- Correctly receives read and connect timeout options.
  [#71](https://github.com/thibaultcha/lua-cassandra/pull/71)
- Log the reason behind retrying a request in the `cluster` module.
  [#71](https://github.com/thibaultcha/lua-cassandra/pull/71)
- Fallback on `listen_address` when `rpc_address` is "bind all" when refreshing
  the cluster nodes with the `cluster` module.
  [#72](https://github.com/thibaultcha/lua-cassandra/pull/72)
- Propagate the CQL version in use when marshalling CQL collection types such
  as map, set, tuple or udt. We now properly marshall such nested CQL values.
  [#73](https://github.com/thibaultcha/lua-cassandra/pull/73)

### [1.0.0] - 2016/07/27

:warning: This release contains **breaking changes**. The library has been
rewritten to greatly increase performance, usability and maintanability. The
result is very pleasant and eleguant: we now offer 2 modules, one "single
host", compatible with PUC Lua 5.1/5.2, and a "cluster" module, greatly
optimized and only compatible with OpenResty.

##### Changed

- New single host `cassandra` module, able to connect to a single Cassandra
node.
- New cluster `resty.cassandra.cluster` module, which leverages the single host
 module and is able to efficiently deal with a multi-nodes Cassandra cluster.
- No more tables as errors. All errors returned by those modules are now
strings.
- Some considerable performance improvements compared to the previous
versions: according to the benchmarks I ran while writing this new
implementation (on a late 2013 Macbook Pro), this new version allows up to
10k q/sec compared to 2k q/s with its old version. I plan on making those
benchmarks available publicly in the near future.

##### Added

- Support for named arguments when binding query parameters (binary protocol
v3).
- Support for client-side timestamps (binary protocol v3).
- Support for query tracing.
- New "datacenter-aware round robin" load balancing policy. This policy will
prioritize nodes from the local datacenter in multi-DC setups.
- A much more complete and reliable test suite (yay!).
- A more complete documentation, including the available policies and better
usage examples.

### [0.5.1] - 2016/04/11

##### Fixed

- Use the `default_port` option when connecting to peers instead of the CQL
default (`9042`). [#49](https://github.com/thibaultCha/lua-cassandra/pull/49)

### [0.5.0] - 2016/02/02

##### Changed

- Following Datastax's model and allowing better flexibility with various C*
providers, authentication now happens with an AuthProvider table that must be
instanciated and passed to a session's options. Example:
    ```lua
    local session, err = cassandra.spawn_session {
      shm = "...",
      contact_points = {...},
      username = "username",
      password = "password"
    }
    ```

    Becomes:
    ```lua
    local session, err = cassandra.spawn_session {
      shm = "...",
      contact_points = {...},
      auth = cassandra.auth.PlainTextProvider("username", "password")
    }
    ```

    The `cassandra` module contains AuthProviders in `cassandra.auth`.
    Only `PlainTextProvider` is currently implemented.

- Remove the `set_log_lvl()` function from the `cassandra` module.

##### Added

- Stronger test suite and new specs for SSL and Authentication.

### [0.4.2] - 2016/01/16

##### Fixed

- Correct timeout check for schema consensus. Prior to this, schema consensus
systematically timed out after 0.5s.
[#24](https://github.com/thibaultCha/lua-cassandra/pull/24)

##### Added

- More documentation.
- Stronger test suite and CI.

### [0.4.1] - 2015/12/18

##### Fixed

- Compatibility for C* < 2.1.6. The query to retrieve the local node's details
does not rely on the existance of an `rpc_address` field anymore, since that
field was only added in 2.1.6.
See https://issues.apache.org/jira/browse/CASSANDRA-9436.

### [0.4.0] - 2015/12/17

Complete rewrite of the driver, to the exception of the serializers.

##### Breaking changes

This release is a complete breaking change with previous versions of the
driver. [#15](https://github.com/thibaultCha/lua-cassandra/pull/15)

##### Added

- Cluster topology auto detection. `contact_points` are not used as the only
available nodes anymore but as entry point to discover the cluster's topology.
- Cluster awareness capabilities. The driver is now capable of keeping track
of which nodes are healthy or unhealthy.
- Load balancing, reconnection, retry and address resolution policies. Only
one of each is currently implemented.
  - Load balancing: shared round-robin accross all workers. Used to
  load-balance the queries in the cluster.
  - Reconnection: shared exponential (exponential reconnection time shared
  accross all workers). Used to determine when an unhealthy node should be
  retried.
  - Retry: a basic retry policy. Used to determine which queries to retry or
  throw errors.
  - Address resolution: a basic address resolution policy. Used to resolve
  `rpc_address` fields.
- Waiting for schema consensus between nodes on `SCHEMA_CHANGE` (DML queries).
- Many more options, configurable per session/query (queries can be executed
with options overriding the session's option).
- Complete abstraction of prepared queries. A simple option to `execute()`
will handle the query preparation. If a node throws an `UNPREPARED` error, the
 query will be prepared and retried seamlessly.
- Stronger test suite. Unit/integration tests with Busted, and ngx_lua
integration tests with Test::Nginx Perl module. Travis-CI jobs are also
faster and more reliable, and run all test suites.
- Binary protocol auto-detection: downgrade from 3 to 2 automatically when
using C* 2.0.
- Compatible with Lua 5.1, 5.2, 5.3, LuaJIT.
- Overall, a better architecture for a better maintainability.

##### Unchanged

- Still optimized for ngx_lua (cosocket API) and plain Lua (with LuaSocket).
- TLS client-to-node encryption and Authentication (PasswordAuthenticator)
are still supported.
- The serializers stayed the same (even if their architecture was rewritten).

##### Removed

- No more support for query tracing (will be added back later).

### [0.3.6] - 2015/08/06

##### Added

- Better error handling in case of CA certificate error with Luasocket.
- Port number in addition to host in socket errors.

##### Fixed

- Shuffling of contact points array.

### [0.3.5] - 2015/07/15

##### Added

- Expose consistency and batch types constants in `cassandra`.
- Better SSL handshake error handling.

### [0.3.3] - 2015/07/14

##### Fixed

- Binary protocol v3 accidentally used v2 encoding methods.
- Startup message accidentally being sent for already established connections
(with reusable sockets).

### [0.3.0] - 2015/07/07

##### Added

- Support for SSL encryption (client/server).

##### Fixed

- IPv6 addresses encoding.

### [0.2.0] - 2015/07/07

##### Added

- Support for authentication (Password Authenticator).

### 0.1.0 - 2015/07/03

Initial release. Forked from jbochi/lua-resty-cassandra v0.5.7 with some
additional features and bug fixes.

##### Added

- Support for both binary protocols v2 and v3.

##### Changed

- More friendly support of auto pagination. THe loop doesn't require as many
parameters.
- OOP style in order to support both binary protocols. `cassandra.new()` must
now be called with `:`.

##### Fixed

- `set_keyspace` erroring on names with capital letters.

[1.5.2]: https://github.com/thibaultCha/lua-cassandra/compare/1.5.1...1.5.2
[1.5.1]: https://github.com/thibaultCha/lua-cassandra/compare/1.5.0...1.5.1
[1.5.0]: https://github.com/thibaultCha/lua-cassandra/compare/1.4.0...1.5.0
[1.4.0]: https://github.com/thibaultCha/lua-cassandra/compare/1.3.4...1.4.0
[1.3.4]: https://github.com/thibaultCha/lua-cassandra/compare/1.3.3...1.3.4
[1.3.3]: https://github.com/thibaultCha/lua-cassandra/compare/1.3.2...1.3.3
[1.3.2]: https://github.com/thibaultCha/lua-cassandra/compare/1.3.1...1.3.2
[1.3.1]: https://github.com/thibaultCha/lua-cassandra/compare/1.3.0...1.3.1
[1.3.0]: https://github.com/thibaultCha/lua-cassandra/compare/1.2.3...1.3.0
[1.2.3]: https://github.com/thibaultCha/lua-cassandra/compare/1.2.2...1.2.3
[1.2.2]: https://github.com/thibaultCha/lua-cassandra/compare/1.2.1...1.2.2
[1.2.1]: https://github.com/thibaultCha/lua-cassandra/compare/1.2.0...1.2.1
[1.2.0]: https://github.com/thibaultCha/lua-cassandra/compare/1.1.1...1.2.0
[1.1.1]: https://github.com/thibaultCha/lua-cassandra/compare/1.1.0...1.1.1
[1.1.0]: https://github.com/thibaultCha/lua-cassandra/compare/1.0.0...1.1.0
[1.0.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.5.1...1.0.0
[0.5.1]: https://github.com/thibaultCha/lua-cassandra/compare/0.5.0...0.5.1
[0.5.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.4.2...0.5.0
[0.4.2]: https://github.com/thibaultCha/lua-cassandra/compare/0.4.1...0.4.2
[0.4.1]: https://github.com/thibaultCha/lua-cassandra/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.6...0.4.0
[0.3.6]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.5...0.3.6
[0.3.5]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.3...0.3.5
[0.3.3]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.0...0.3.3
[0.3.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.1.0...0.2.0
