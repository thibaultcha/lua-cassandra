local cql = require "cassandra.cql2"
local Buffer = cql.buffer


describe("Buffer", function()
    -- protocol types (different than CQL types)
    local fixtures  = {
        byte        = { 1, 2, 3 },
        int         = { 0, 4200, -42, -1234, 2147483647, --[[-2147483648]] },
        long        = { 0, 4200, 9223372036854775807 },
        short       = { 0, 1, -1, 12, 13, 0, --[[65535]] },
        string      = { "hello world" },
        long_string = { string.rep("blob", 1000), "" },
        uuid        = { "1144bada-852c-11e3-89fb-e0b9a54a6d11" },
        string_list = {
            { "hello", "world" },
            {},
        },

        inet = {
            "127.0.0.1", "0.0.0.1", "8.8.8.8", "255.255.255.255",
            "2001:0db8:85a3:0042:1000:8a2e:0370:7334",
            "2001:0db8:0000:0000:0000:0000:0000:0001"
        },

        string_map = {
            { hello = "world" },
            { cql_version = "3.0.0", foo = "bar" }
        },

        string_multimap = {
            { hello = { "world", "universe" } },
            { foo   = { "bar", "baz" } },
        },
    }


    it("[bytes] notation", function()
        local buf_w = Buffer.new_w()
        Buffer.write_bytes(buf_w, "foo")
        local buf_r = Buffer.new_r(nil, Buffer.get(buf_w))
        assert.equal("foo", Buffer.read_bytes(buf_r))

        -- empty bytes
        buf_w = Buffer.new_w()
        Buffer.write_bytes(buf_w, "")
        buf_r = Buffer.new_r(nil, Buffer.get(buf_w))
        assert.equal("", Buffer.read_bytes(buf_r))

        -- cql null
        buf_w = Buffer.new_w()
        Buffer.write_null(buf_w)
        buf_r = Buffer.new_r(nil, Buffer.get(buf_w))
        assert.equal(cql.cql_t_null, Buffer.read_bytes(buf_r))
    end)


    it("[value] notation", function()
        local buf_w = Buffer.new_w()
        Buffer.write_value(buf_w, "foo")
        local buf_r = Buffer.new_r(nil, Buffer.get(buf_w))
        assert.equal("foo", Buffer.read_value(buf_r))

        -- cql null
        buf_w = Buffer.new_w()
        Buffer.write_null(buf_w)
        buf_r = Buffer.new_r(nil, Buffer.get(buf_w))
        assert.equal(cql.cql_t_null, Buffer.read_value(buf_r))

        -- cql unset
        buf_w = Buffer.new_w()
        Buffer.write_unset(buf_w)
        buf_r = Buffer.new_r(nil, Buffer.get(buf_w))
        assert.equal(cql.cql_t_unset, Buffer.read_value(buf_r))
    end)


    it("[short bytes] notation", function()
        local buf_w = Buffer.new_w()
        Buffer.write_short_bytes(buf_w, "foo")
        local buf_r = Buffer.new_r(nil, Buffer.get(buf_w))
        assert.equal("foo", Buffer.read_short_bytes(buf_r))

        -- empty bytes
        buf_w = Buffer.new_w()
        Buffer.write_short_bytes(buf_w, "")
        buf_r = Buffer.new_r(nil, Buffer.get(buf_w))
        assert.equal("", Buffer.read_short_bytes(buf_r))
    end)


    -- [option]
    -- [option list]


    for fixture_type, fixture_values in pairs(fixtures) do

        it("[" .. fixture_type .. "] notation", function()
            for _, fixture in ipairs(fixture_values) do
                -- write buffer
                local buf_w = Buffer.new_w()

                Buffer["write_" .. fixture_type](buf_w, fixture)
                local bytes = Buffer.get(buf_w)
                assert.is_string(bytes)

                -- read buffer
                local buf_r = Buffer.new_r(nil, bytes)
                local decoded = Buffer["read_" .. fixture_type](buf_r)

                if type(fixture) == "table" then
                    assert.same(fixture, decoded)

                else
                    assert.equal(fixture, decoded)
                end
            end
        end)

    end


    it("[bytes map] notation", function()
        local buf_w = Buffer.new_w()
        Buffer.write_short(buf_w, 2)
        Buffer.write_string(buf_w, "foo")
        Buffer.write_bytes(buf_w, "123")
        Buffer.write_string(buf_w, "bar")
        Buffer.write_bytes(buf_w, "456")

        local buf_r = Buffer.new_r(nil, Buffer.get(buf_w))
        local map = Buffer.read_bytes_map(buf_r)

        assert.same({
            foo = "123",
            bar = "456",
        }, map)
    end)


    describe("[inet] notation", function()
        local fixtures = {
            ["2001:0db8:85a3:0042:1000:8a2e:0370:7334"] = "2001:0db8:85a3:0042:1000:8a2e:0370:7334",
            ["2001:0db8:0000:0000:0000:0000:0000:0001"] = "2001:db8::1",
            ["2001:0db8:85a3:0000:0000:0000:0000:0010"] = "2001:db8:85a3::10",
            ["2001:0db8:85a3:0000:0000:0000:0000:0100"] = "2001:db8:85a3::100",
            ["0000:0000:0000:0000:0000:0000:0000:0001"] = "::1",
            ["0000:0000:0000:0000:0000:0000:0000:0000"] = "::"
        }


        it("shortens ipv6 addresses", function()
            for expected_ip, fixture_ip in pairs(fixtures) do
                local buf_w = Buffer.new_w()
                Buffer.write_inet(buf_w, fixture_ip)

                local buf_r = Buffer.new_r(nil, Buffer.get(buf_w))
                assert.equal(expected_ip, Buffer.read_inet(buf_r))
            end
        end)
    end)
end)
