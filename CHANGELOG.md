### [Unreleased][unreleased]

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

[unreleased]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.6...HEAD
[0.3.6]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.5...0.3.6
[0.3.5]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.3...0.3.5
[0.3.3]: https://github.com/thibaultCha/lua-cassandra/compare/0.3.0...0.3.3
[0.3.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/thibaultCha/lua-cassandra/compare/0.1.0...0.2.0
