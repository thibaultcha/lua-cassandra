local options = require "cassandra.options"
local parse = options.parse

describe("options", function()
  describe("parse()", function()
    it("requires shm", function()
      local _, err = parse {}
      assert.equal("shm is required", err)
    end)
    it("requires contact_points", function()
      local _, err = parse {shm = "cassandra"}
      assert.equal("contact_points is required", err)
    end)
    it("returns default options", function()
      local opts, err = parse {
        shm = "cassandra",
        contact_points = {"127.0.0.1"},
        query_options = {
          prepare = true
        }
      }
      assert.falsy(err)
      assert.is_table(opts.policies)
      assert.is_table(opts.query_options)
      assert.is_table(opts.protocol_options)
      assert.is_table(opts.socket_options)
      assert.is_table(opts.ssl_options)
      assert.equal(1000, opts.query_options.page_size)
      assert.equal("cassandra", opts.shm)
      assert.True(opts.query_options.prepare)
    end)
  end)

  describe("extend_query_options()", function()
    local options = parse {
      shm = "test",
      keyspace = "my_keyspace",
      contact_points = {"127.0.0.1", "127.0.0.2"}
    }

    it("override provided options", function()
      local query_options = options:extend_query_options {page_size = 1000, prepare = true}
      assert.same({
        auto_paging = false,
        consistency = 1,
        page_size = 1000,
        prepare = true,
        retry_on_timeout = true,
        serial_consistency = 8
      }, query_options)
    end)
    it("higher level override priority", function()
      local query_options = options:extend_query_options({page_size = 1000, prepare = true}, {page_size = 10})
      assert.same({
        auto_paging = false,
        consistency = 1,
        page_size = 10,
        prepare = false,
        retry_on_timeout = true,
        serial_consistency = 8
      }, query_options)
    end)
    it("is not confused by booleans", function()
      local query_options = options:extend_query_options({retry_on_timeout = false})
      assert.same({
        auto_paging = false,
        consistency = 1,
        page_size = 1000,
        prepare = false,
        retry_on_timeout = false,
        serial_consistency = 8
      }, query_options)
    end)
  end)
end)
