### [Unreleased][unreleased]

### [0.5.2] - 2016/04/19

##### Added

- Better error messages for SSL and authentication errors.

### [0.5.1] - 2016/04/11

##### Fixed

- Use the `default_port` option when connecting to peers instead of the CQL default (`9042`). [#49](https://github.com/thibaultCha/lua-cassandra/pull/49)

### [0.5.0] - 2016/02/02

##### Changed

- Following Datastax's model and allowing better flexibility with various C* providers, authentication now happens with an AuthProvider table that must be instanciated and passed to a session's options. Example:
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

    The `cassandra` module contains AuthProviders in `cassandra.auth`. Only `PlainTextProvider` is currently implemented.

- Remove the `set_log_lvl()` function from the `cassandra` module.

##### Added

- Stronger test suite and new specs for SSL and Authentication.

### [0.4.2] - 2016/01/16

##### Fixed

- Correct timeout check for schema consensus. Prior to this, schema consensus systematically timed out after 0.5s. [#24](https://github.com/thibaultCha/lua-cassandra/pull/24)

##### Added

- More documentation.
- Stronger test suite and CI.

### [0.4.1] - 2015/12/18

##### Fixed

- Compatibility for C* < 2.1.6. The query to retrieve the local node's details does not rely on the existance of an `rpc_address` field anymore, since that field was only added in 2.1.6. See https://issues.apache.org/jira/browse/CASSANDRA-9436.

### [0.4.0] - 2015/12/17

Complete rewrite of the driver, to the exception of the serializers.

##### Breaking changes

This release is a complete breaking change with previous versions of the driver. [#15](https://github.com/thibaultCha/lua-cassandra/pull/15)

##### Added

- Cluster topology auto detection. `contact_points` are not used as the only available nodes anymore but as entry point to discover the cluster's topology.
- Cluster awareness capabilities. The driver is now capable of keeping track of which nodes are healthy or unhealthy.
- Load balancing, reconnection, retry and address resolution policies. Only one of each is currently implemented.
  - Load balancing: shared round-robin accross all workers. Used to load-balance the queries in the cluster.
  - Reconnection: shared exponential (exponential reconnection time shared accross all workers). Used to determine when an unhealthy node should be retried.
  - Retry: a basic retry policy. Used to determine which queries to retry or throw errors.
  - Address resolution: a basic address resolution policy. Used to resolve `rpc_address` fields.
- Waiting for schema consensus between nodes on `SCHEMA_CHANGE` (DML queries).
- Many more options, configurable per session/query (queries can be executed with options overriding the session's option).
- Complete abstraction of prepared queries. A simple option to `execute()` will handle the query preparation. If a node throws an `UNPREPARED` error, the query will be prepared and retried seamlessly.
- Stronger test suite. Unit/integration tests with Busted, and ngx_lua integration tests with Test::Nginx Perl module. Travis-CI jobs are also faster and more reliable, and run all test suites.
- Binary protocol auto-detection: downgrade from 3 to 2 automatically when using C* 2.0.
- Compatible with Lua 5.1, 5.2, 5.3, LuaJIT.
- Overall, a better architecture for a better maintainability.

##### Unchanged

- Still optimized for ngx_lua (cosocket API) and plain Lua (with LuaSocket).
- TLS client-to-node encryption and Authentication (PasswordAuthenticator) are still supported.
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
- Startup message accidentally being sent for already established connections (with reusable sockets).

### [0.3.0] - 2015/07/07

##### Added

- Support for SSL encryption (client/server).

##### Fixed

- IPv6 addresses encoding.

### [0.2.0] - 2015/07/07

##### Added

- Support for authentication (Password Authenticator).

### 0.1.0 - 2015/07/03

Initial release. Forked from jbochi/lua-resty-cassandra v0.5.7 with some additional features and bug fixes.

##### Added

- Support for both binary protocols v2 and v3.

##### Changed

- More friendly support of auto pagination. THe loop doesn't require as many parameters.
- OOP style in order to support both binary protocols. `cassandra.new()` must now be called with `:`.

##### Fixed

- `set_keyspace` erroring on names with capital letters.

[unreleased]: https://github.com/thibaultCha/lua-cassandra/compare/0.5.0...HEAD
[0.5.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.4.2...0.5.0
[0.4.2]: https://github.com/thibaultCha/lua-cassandra/compare/0.4.1...0.4.2
[0.4.1]: https://github.com/thibaultCha/lua-cassandra/compare/0.4.0...0.4.1
[0.4.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.6...0.4.0
[0.3.6]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.5...0.3.6
[0.3.5]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.3...0.3.5
[0.3.3]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.0...0.3.3
[0.3.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.1.0...0.2.0
