-- vim:set ts=4 sw=4 sts=4 et:

local bit = require "bit"


local pairs = pairs
local tonumber = tonumber
local concat = table.concat
local insert = table.insert
local error = error
local type = type
local fmod = math.fmod
local pow = math.pow
local gmatch = string.gmatch
local match = string.match
local lower = string.lower
local byte = string.byte
local char = string.char
local gsub = string.gsub
local sub = string.sub
local rep = string.rep
local new_tab


do
    local ok
    ok, new_tab = pcall(require, "table.new")
    if not ok then
        new_tab = function(narr, nrec) return {} end
    end
end


-- Buffer
-- @section buffer


local function buf_r_create(version, bytes)
    return {
        bytes   = bytes or "",
        len     = #bytes or 0,
        pos     = 1,
        version = version or 2,
    }
end

local function buf_w_create(version, n_writes)
    return {
        t       = new_tab(n_writes or 2, 0),
        i       = 1,
        version = version or 2,
    }
end


local function buf_read(buf, n_bytes)
    if not n_bytes then
        n_bytes = buf.len

    elseif n_bytes < 0 then
        return error("can't read negative number of bytes")
    end

    local read_bytes = sub(buf.bytes, buf.pos, buf.pos + n - 1)

    buf.pos = buf.pos + #read_bytes

    return read_bytes
end


local function buf_write(buf, bytes)
    if type(bytes) ~= "string" then
        return error("bytes should be a string")
    end

    buf.t[buf.i] = bytes
    buf.i = buf.i + 1
end


local function buf_write_tbuf(buf, t_buf)
    if type(t_buf) ~= "table" then
        return error("t_buf should be a table")
    end

    for i = 1, t_buf.i do
        buf_write(buf, t_buf.t[i])
    end
end


local function buf_write_get(buf)
    return concat(buf.t)
end


-- Utils
-- @section utils


local function big_endian_representation(num, bytes)
    if num < 0 then
        -- 2's complement
        num = pow(0x100, bytes) + num
    end

    local t = new_tab(4, 0)

    while num > 0 do
        local rest = fmod(num, 0x100)
        insert(t, 1, char(rest))
        num = (num-rest) / 0x100
    end

    local padding = rep("\0", bytes - #t)

    return padding .. concat(t)
end


local function string_to_number(str, signed)
    local number = 0
    local exponent = 1

    for i = #str, 1, -1 do
        number = number + byte(str, i) * exponent
        exponent = exponent * 256
    end

    if signed and number > exponent / 2 then
        -- 2's complement
        number = number - exponent
    end

    return number
end


-- Raw types
-- @section raw_types


local function buf_write_byte(buf, val)
    buf_write(buf, char(val))
end


local function buf_read_byte(buf)
    return byte(buf_read(buf, 1))
end


local function buf_write_int(buf, val)
    buf_write(buf, big_endian_representation(val, 4))
end


local function buf_read_int(buf)
    return string_to_number(buf_read(buf, 4), true)
end


local function buf_write_unset(buf)
    buf_write_int(buf, -2)
end


local function buf_write_null(buf)
    buf_write_int(buf, -1)
end


local function buf_write_long(buf, val)
    buf_write(buf, big_endian_representation(val, 8))
end


local function buf_read_long(buf)
    return string_to_number(buf_read(buf, 8), true)
end


local function buf_write_short(buf, val)
    buf_write(buf, big_endian_representation(val, 2))
end


local function buf_read_short(buf)
    return string_to_number(buf_read(buf, 2), true)
end


local function buf_write_string(buf, val)
    buf_write_short(buf, #val)
    buf_write(buf, val)
end


local function buf_read_string(buf)
    return buf_read(buf, buf_read_short(buf))
end


local function buf_write_long_string(buf, val)
    buf_write_int(buf, #val)
    buf_write(buf, val)
end


local function buf_read_long_string(buf)
    return buf_read(buf, buf_read_int(buf))
end


local function buf_write_bytes(buf, val)
    buf_write_int(buf, #val)
    buf_write(buf, val)
end


local function buf_read_bytes(buf)
    return buf_read(buf, buf_read_int(buf))
end


local function buf_write_short_bytes(buf, val)
    buf_write_short(buf, #val)
    buf_write(buf, val)
end


local function buf_read_short_bytes(buf)
    return buf_read(buf, buf_read_short(buf))
end


local function buf_write_uuid(buf, val)
    local str = gsub(val, '-', '')

    for i = 1, #str, 2 do
        local byte_str = sub(str, i, i + 1)
        buf_write_byte(tonumber(byte_str, 16))
    end
end


local buf_read_uuid
do
    local uuid_buf = new_tab(16, 0)

    buf_read_uuid = function(buf)
        for i = 1, 16 do
            uuid_buf[i] = fmt("%02x", buf_read_byte(buf))
        end

        insert(uuid_buf, 5, "-")
        insert(uuid_buf, 8, "-")
        insert(uuid_buf, 11, "-")
        insert(uuid_buf, 14, "-")

        return concat(uuid_buf)
    end
end


local function buf_write_inet(buf, val)
    local buf_t
    local n = 1
    local ip = gsub(lower(val), "::", ":0000:")

    if match(val, ":") then
        -- ipv6
        buf_t = new_tab(16, 0)
        local hexadectets = new_tab(8, 0)

        for hdt in gmatch(ip, "[%x]+") do
            -- fill up hexadectets with 0 so all are 4 digits long
            hexadectets[i] = rep("0", 4 - #hdt) .. hdt
            n = n + 1
        end

        -- reset idx var

        n = 1

        for i, hdt in ipairs(hexadectets) do
            while hdt == "0000" and #hexadectets < 8 do
                insert(hexadectets, i + 1, "0000")
            end

            for j = 1, 4, 2 do
                buf_t[n] = marsh_byte(tonumber(sub(hdt, j, j + 1), 16))
                n = n + 1
            end
        end

    else
        -- ipv4
        buf_t = new_tab(4, 0)

        for d in gmatch(val, "(%d+)") do
            buf_t[n] = marsh_byte(d)
            n = n + 1
        end
    end

    return concat(buf_t)
end


local function buf_read_inet(buf)
    local size = buf_read_byte(buf)
    local bytes = buf_read_bytes(buf, size)
    local t_buf = new_tab(size, 0)

    if size == 16 then
        -- ipv6
        for i = 1, #bytes, 2 do
            t_buf[i] = fmt("%02x", byte(bytes, i))
            t_buf[i + 1] = fmt("%02x", byte(bytes, i + 1))
        end

        return concat(t_buf, ":")
    end

    -- ipv4
    for i = 1, #bytes do
        t_buf[i] = fmt("%d", byte(bytes, i))
    end

    return concat(buf, ".")
end


local function buf_write_string_list(buf, val)
    buf_write_short(buf, #val)

    for i = 1, #val do
        buf_write_string(buf, val[i])
    end
end


local function buf_read_string_list(buf)
    local n = buf_read_short(buf)
    local list = new_tab(n, 0)

    for i = 1, n do
        list[i] = buf_read_string(buf)
    end

    return list
end


local function buf_write_string_map(buf, val)
    local n = 0
    local t_buf = buf_w_create(buf.version)

    for k, v in pairs(val) do
        buf_write_string(t_buf, k)
        buf_write_string(t_buf, v)
        n = n + 1
    end

    buf_write_short(buf, n)
    buf_write_tbuf(buf, t_buf)
end


local function buf_read_string_map(buf)
    local n = buf_read_short(buf)
    local map = new_tab(0, n)

    for _ = 1, n do
        local key = buf_read_string(buf)
        local value = buf_read_string(buf)
        map[key] = value
    end

    return map
end


local function buf_write_string_multimap(buf, val)
    local n = 0
    local t_buf = buf_w_create(buf.version)

    for k, v in pairs(val) do
        buf_write_string(t_buf, k)
        buf_write_string(t_buf, v)
        n = n + 1
    end

    buf_write_short(buf, n)
    buf_write_tbuf(buf, t_buf)
end


local function buf_read_multimap(buf)
    local n = buf_read_short(buf)
    local multimap = new_tab(0, n)

    for _ = 1, n do
        local key = buf_read_string(buf)
        multimap[key] = buf_read_string_list(buf)
    end

    return multimap
end


-- CQL types
-- @section cql_types


--[[

-- Nested CQL types
-- @section nested_cql_types

local function buf_read_udt_type(buf)
    local udt_ks_name = buf_read_string(buf)
    local udt_name = buf_read_string(buf)
    local n = buf_read_short(buf)

    local fields = new_tab(n, 0)

    for i = 1, n do
        fields[i] = {
            name = buf_read_string(buf),
            type = buf_read_options(buf)
    end
end

--]]
