local utils = require "spec.spec_utils"
local host = require "cassandra.host"

describe("host", function()
  setup(function()
    utils.ccm_start()
  end)

  describe("new()", function()
    it("create a peer", function()
      local peer, err = host.new()
      assert.falsy(err)
      assert.equal("127.0.0.1", peer.host)
      assert.equal(9042, peer.port)
      assert.equal(3, peer.protocol_version)
      assert.falsy(peer.ssl)
      assert.truthy(peer.sock)
    end)
    it("accepts options", function()
      local peer, err = host.new {
        host = "192.168.1.1",
        port = 9043,
        protocol_version = 2
      }
      assert.falsy(err)
      assert.equal("192.168.1.1", peer.host)
      assert.equal(9043, peer.port)
      assert.equal(2, peer.protocol_version)
      assert.falsy(peer.ssl)
      assert.truthy(peer.sock)
    end)
  end)

  describe("connect()", function()
    it("errors if not initialized", function()
      local ok, err = host:connect()
      assert.equal("no socket created", err)
      assert.falsy(ok)
    end)
    it("connects to a peer", function()
      local peer, err = host.new()
      assert.falsy(err)

      local ok, err = peer:connect()
      assert.falsy(err)
      assert.True(ok)
    end)
  end)

  describe("close()", function()
    it("errors if not initialized", function()
      local ok, err = host:close()
      assert.equal("no socket created", err)
      assert.falsy(ok)
    end)
    it("closes a peer", function()
      local peer, err = host.new()
      assert.falsy(err)

      local ok, err = peer:connect()
      assert.falsy(err)
      assert.True(ok)

      ok, err = peer:close()
      assert.falsy(err)
      assert.equal(1, ok)
    end)
  end)

  describe("settimeout()", function()
    it("errors if not initialized", function()
      local ok, err = host:settimeout()
      assert.equal("no socket created", err)
      assert.falsy(ok)
    end)
    it("sets socket timeout", function()
      local peer, err = host.new()
      assert.falsy(err)

      local ok, err = peer:connect()
      assert.falsy(err)
      assert.True(ok)

      assert.has_no_error(function()
        peer:settimeout(1000)
      end)

      finally(function()
        peer:close()
      end)
    end)
  end)

  describe("setkeepalive()", function()
    it("errors if not initialized", function()
      local ok, err = host:setkeepalive()
      assert.equal("no socket created", err)
      assert.falsy(ok)
    end)
    it("sets socket timeout", function()
      local peer, err = host.new()
      assert.falsy(err)

      local ok, err = peer:connect()
      assert.falsy(err)
      assert.True(ok)

      local ok, err = peer:setkeepalive()
      assert.falsy(err)
      assert.True(ok)

      finally(function()
        peer:close()
      end)
    end)
  end)

  describe("CQL", function()
    local peer
    setup(function()
      local p, err = host.new()
      assert.falsy(err)

      local _, err = p:connect()
      assert.falsy(err)

      peer = p
    end)
    teardown(function()
      peer:close()
    end)

    describe("execute()", function()
      it("errors if not initialized", function()
        local ok, err = host:execute()
        assert.equal("no socket created", err)
        assert.falsy(ok)
      end)
      it("executes a CQL query", function()
        local rows, err, code = peer:execute "SELECT * FROM system.local"
        assert.falsy(err)
        assert.falsy(code)
        assert.equal(1, #rows)
        assert.equal("ROWS", rows.type)

        local row = rows[1]
        assert.equal("local", row.key)
      end)
      it("parses SCHEMA_CHANGE results", function()
        local tmp_name = os.tmpname():gsub("/", ""):lower()
        local res, err = peer:execute([[
          CREATE KEYSPACE IF NOT EXISTS ]]..tmp_name..[[
          WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}
        ]])
        assert.falsy(err)
        assert.equal(0, #res)
        assert.equal("SCHEMA_CHANGE", res.type)
        assert.equal("CREATED", res.change_type)
        assert.equal("KEYSPACE", res.target)
        assert.equal(tmp_name, res.keyspace)
        assert.falsy(res.name)

        res, err = peer:execute([[
          CREATE TABLE ]]..tmp_name..[[.my_table(
            id uuid PRIMARY KEY,
            value int
          )
        ]])
        assert.falsy(err)
        assert.equal(0, #res)
        assert.equal("SCHEMA_CHANGE", res.type)
        assert.equal("CREATED", res.change_type)
        assert.equal("TABLE", res.target)
        assert.equal(tmp_name, res.keyspace)
        assert.equal("my_table", res.name)

        res, err = peer:execute("DROP KEYSPACE "..tmp_name)
        assert.falsy(err)
        assert.equal(0, #res)
        assert.equal("SCHEMA_CHANGE", res.type)
        assert.equal("DROPPED", res.change_type)
        assert.equal("KEYSPACE", res.target)
        assert.equal(tmp_name, res.keyspace)
        assert.falsy(res.name)
      end)
      it("parses SET_KEYSPACE results", function()
        local peer_k, err = host.new()
        assert.falsy(err)
        local _, err = peer_k:connect()
        assert.falsy(err)

        local tmp_name = os.tmpname():gsub("/", ""):lower()
        local res, err = peer_k:execute([[
          CREATE KEYSPACE IF NOT EXISTS ]]..tmp_name..[[
          WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}
        ]])
        assert.falsy(err)

        res, err = peer_k:execute(string.format('USE "%s"', tmp_name))
        assert.falsy(err)
        assert.equal(0, #res)
        assert.equal("SET_KEYSPACE", res.type)
        assert.equal(tmp_name, res.keyspace)

        res, err = peer_k:execute("DROP KEYSPACE "..tmp_name)
        assert.falsy(err)
      end)
      it("returns CQL errors", function()
        local rows, err, code = peer:execute "SELECT"
        assert.falsy(rows)
        assert.equal("[Syntax error] line 0:-1 no viable alternative at input '<EOF>'", err)
        assert.equal(host.cql_errors.SYNTAX_ERROR, code)
      end)
      it("binds args", function()
        local rows, err = peer:execute("SELECT * FROM system.local WHERE key = ?", {"local"})
        assert.falsy(err)
        assert.equal("local", rows[1].key)
      end)
    end) -- execute()

    describe("prepared queries", function()
      it("should prepare a query", function()
        local res, err = peer:prepare "SELECT * FROM system.local WHERE key = ?"
        assert.falsy(err)
        assert.truthy(res.query_id)
        assert.equal("PREPARED", res.type)
      end)
      it("should execute a prepared query", function()
        local res, err = peer:prepare "SELECT * FROM system.local WHERE key = ?"
        assert.falsy(err)

        local rows, err = peer:execute(res.query_id, {"local"}, {prepared = true})
        assert.falsy(err)
        assert.equal("local", rows[1].key)
      end)
    end)

    describe("batch()", function()

    end)

    describe("set_keyspace()", function()
      it("sets a peer's keyspace", function()
        local peer_k, err = host.new()
        assert.falsy(err)
        local _, err = peer_k:connect()
        assert.falsy(err)

        local res, err = peer_k:set_keyspace "system"
        assert.falsy(err)
        assert.equal(0, #res)
        assert.equal("SET_KEYSPACE", res.type)
        assert.equal("system", res.keyspace)

        local rows, err = peer_k:execute "SELECT * FROM local"
        assert.falsy(err)
        assert.equal("local", rows[1].key)
      end)
    end)
  end) -- CQL

  describe("__tostring()", function()
    it("has a __tostring() metamethod", function()
      local peer, err = host.new()
      assert.falsy(err)

      local str = tostring(peer)
      assert.truthy(string.find(str, "<Cassandra socket: tcp{master}:"))
    end)
  end)
end)
