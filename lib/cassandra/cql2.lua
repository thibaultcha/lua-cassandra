-- vim:set ts=4 sw=4 sts=4 et:

local bit = require "bit"


local pairs = pairs
local tonumber = tonumber
local concat = table.concat
local insert = table.insert
local error = error
local type = type
local huge = math.huge
local frexp = math.frexp
local ldexp = math.ldexp
local floor = math.floor
local fmod = math.fmod
local pow = math.pow
local gmatch = string.gmatch
local match = string.match
local lower = string.lower
local byte = string.byte
local char = string.char
local gsub = string.gsub
local fmt = string.format
local sub = string.sub
local rep = string.rep
local bor = bit.bor
local new_tab


do
    local ok
    ok, new_tab = pcall(require, "table.new")
    if not ok then
        new_tab = function(narr, nrec) return {} end
    end
end


local CQL_T_UNSET = {}
local CQL_T_NULL  = {}
local EMPTY_T     = {}


-- CQL consants
-- @section cql_constants


local cql_types = {
    custom      = 0x00,
    ascii       = 0x01,
    bigint      = 0x02,
    blob        = 0x03,
    boolean     = 0x04,
    counter     = 0x05,
    decimal     = 0x06,
    double      = 0x07,
    float       = 0x08,
    int         = 0x09,
    text        = 0x0A,
    timestamp   = 0x0B,
    uuid        = 0x0C,
    varchar     = 0x0D,
    varint      = 0x0E,
    timeuuid    = 0x0F,
    inet        = 0x10,
    list        = 0x20,
    map         = 0x21,
    set         = 0x22,
    udt         = 0x30,
    tuple       = 0x31,
}


local OP_CODES     = {
    ERROR          = 0x00,
    STARTUP        = 0x01,
    READY          = 0x02,
    AUTHENTICATE   = 0x03,
    OPTIONS        = 0x05,
    SUPPORTED      = 0x06,
    QUERY          = 0x07,
    RESULT         = 0x08,
    PREPARE        = 0x09,
    EXECUTE        = 0x0A,
    REGISTER       = 0x0B,
    EVENT          = 0x0C,
    BATCH          = 0x0D,
    AUTH_CHALLENGE = 0x0E,
    AUTH_RESPONSE  = 0x0F,
    AUTH_SUCCESS   = 0x10,
}


local FRAME_FLAGS    = {
    --COMPRESSION    = 0x01,
    TRACING          = 0x02,
    --CUSTOM_PAYLOAD = 0x04,
    WARNING          = 0x08,
}


local CONSISTENCIES = {
    ANY             = 0X0000,
    ONE             = 0X0001,
    TWO             = 0X0002,
    THREE           = 0X0003,
    QUORUM          = 0X0004,
    ALL             = 0X0005,
    LOCAL_QUORUM    = 0X0006,
    EACH_QUORUM     = 0X0007,
    SERIAL          = 0X0008,
    LOCAL_SERIAL    = 0X0009,
    LOCAL_ONE       = 0X000A,
}


-- Buffer
-- @section buffer


local _M_buf = {}


function _M_buf.new_r(version, bytes)
    return {
        bytes   = bytes or "",
        len     = #bytes or 0,
        pos     = 1,
        version = version or 2,
    }
end


function _M_buf.new_w(version, n_writes)
    return {
        t       = new_tab(n_writes or 2, 0),
        i       = 1,
        version = version or 2,
    }
end


function _M_buf.read(buf, n_bytes)
    if not n_bytes then
        n_bytes = buf.len

    elseif n_bytes < 0 then
        return error("can't read negative number of bytes")
    end

    local read_bytes = sub(buf.bytes, buf.pos, buf.pos + n_bytes - 1)

    buf.pos = buf.pos + #read_bytes

    return read_bytes
end


function _M_buf.write(buf, bytes)
    if type(bytes) ~= "string" then
        return error("bytes should be a string")
    end

    buf.t[buf.i] = bytes
    buf.i = buf.i + 1
end


function _M_buf.copy(buf, t_buf)
    if type(t_buf) ~= "table" then
        return error("t_buf should be a table")
    end

    for i = 1, t_buf.i - 1 do
        _M_buf.write(buf, t_buf.t[i])
    end
end


function _M_buf.get(buf)
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
        num = (num - rest) / 0x100
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


local function is_list(t)
    if type(t) ~= "table" then
        return false
    end

    local i = 0

    for _ in pairs(t) do
        i = i + 1
        if t[i] == nil then
            return false
        end
    end

    return true
end


-- Raw types
-- @section raw_types


-- byte


local function marsh_byte(value)
    return char(value)
end


--[[
local function unmarsh_byte(bytes)
    return byte(bytes)
end
--]]


function _M_buf.write_byte(buf, value)
    _M_buf.write(buf, marsh_byte(value))
end


function _M_buf.read_byte(buf)
    return byte(_M_buf.read(buf, 1))
end


-- int


local function marsh_int(value)
    return big_endian_representation(value, 4)
end


local function unmarsh_int(bytes)
    return string_to_number(bytes, true)
end


function _M_buf.write_int(buf, value)
    _M_buf.write(buf, marsh_int(value))
end


function _M_buf.read_int(buf)
    return unmarsh_int(_M_buf.read(buf, 4))
end


-- unset


local function marsh_unset()
    return marsh_int(-2)
end


function _M_buf.write_unset(buf)
    _M_buf.write(buf, marsh_unset())
end


-- null


local function marsh_null()
    return marsh_int(-1)
end


function _M_buf.write_null(buf)
    _M_buf.write(buf, marsh_null())
end


-- long


local function marsh_long(value)
    return big_endian_representation(value, 8)
end


--[[
local function unmarsh_long(bytes)
    return string_to_number(bytes, true)
end
--]]


function _M_buf.write_long(buf, value)
    _M_buf.write(buf, marsh_long(value))
end


function _M_buf.read_long(buf)
    return string_to_number(_M_buf.read(buf, 8), true)
end


-- short


local function marsh_short(value)
    return big_endian_representation(value, 2)
end


--[[
local function unmarsh_short(bytes)
    return string_to_number(bytes, true)
end
--]]


function _M_buf.write_short(buf, value)
    _M_buf.write(buf, marsh_short(value))
end


function _M_buf.read_short(buf)
    return string_to_number(_M_buf.read(buf, 2), true)
end


-- string


--[[
local function marsh_string(value)
    return marsh_short(#value) .. value
end


local function unmarsh_string(bytes)

end
--]]


function _M_buf.write_string(buf, value)
    _M_buf.write_short(buf, #value)
    _M_buf.write(buf, value)
end


function _M_buf.read_string(buf)
    return _M_buf.read(buf, _M_buf.read_short(buf))
end


-- long string


--[[
local function marsh_long_string(value)
    return marsh_int(#value) .. value
end


local function unmarsh_long_string(bytes)

end
--]]


function _M_buf.write_long_string(buf, value)
    _M_buf.write_int(buf, #value)
    _M_buf.write(buf, value)
end


function _M_buf.read_long_string(buf)
    return _M_buf.read(buf, _M_buf.read_int(buf))
end


-- bytes


local function marsh_bytes(value)
    return marsh_int(#value) .. value
end


--[[
local function unmarsh_bytes(bytes)

end
--]]


function _M_buf.write_bytes(buf, value)
    _M_buf.write_int(buf, #value)
    _M_buf.write(buf, value)
end


function _M_buf.read_bytes(buf)
    local n = _M_buf.read_int(buf)
    if n >= 0 then
        return _M_buf.read(buf, n)
    end

    return CQL_T_NULL
end


-- value


_M_buf.write_value = _M_buf.write_bytes


function _M_buf.read_value(buf)
    local n = _M_buf.read_int(buf)
    if n >= 0 then
        return _M_buf.read(buf, n)
    end

    if n == -1 then
        return CQL_T_NULL
    end

    if n == -2 then
        return CQL_T_UNSET
    end

    return error("n component of [value] notation is invalid: " .. n)
end


-- short_bytes


--[[
local function marsh_short_bytes(value)
    return marsh_short(#value) .. value
end


local function unmarsh_short_bytes(bytes)

end
--]]


function _M_buf.write_short_bytes(buf, value)
    _M_buf.write_short(buf, #value)
    _M_buf.write(buf, value)
end


function _M_buf.read_short_bytes(buf)
    return _M_buf.read(buf, _M_buf.read_short(buf))
end


-- uuid


local marsh_uuid
local unmarsh_uuid
do
    local uuid_buf = new_tab(20, 0)

    marsh_uuid = function(value)
        local str = gsub(value, "-", "")
        local n = 1

        for i = 1, 32, 2 do
            local b = sub(str, i, i + 1)
            uuid_buf[n] = marsh_byte(tonumber(b, 16))
            n = n + 1
        end

        return concat(uuid_buf)
    end

    unmarsh_uuid = function(bytes)
        uuid_buf[20] = nil
        uuid_buf[19] = nil
        uuid_buf[18] = nil
        uuid_buf[17] = nil

        for i = 1, 16 do
            local b = sub(bytes, i, i + 1)
            uuid_buf[i] = fmt("%02x", byte(b))
        end

        insert(uuid_buf, 5, "-")
        insert(uuid_buf, 8, "-")
        insert(uuid_buf, 11, "-")
        insert(uuid_buf, 14, "-")

        return concat(uuid_buf)
    end
end


function _M_buf.write_uuid(buf, value)
    _M_buf.write(buf, marsh_uuid(value))
end


function _M_buf.read_uuid(buf)
    return unmarsh_uuid(_M_buf.read(buf, 16))
end


-- inet
-- FIXME: review initial [byte] for size and [int] for port


local function marsh_inet(value)
    local buf_t
    local n

    if match(value, ":") then
        -- ipv6
        buf_t = new_tab(16 + 2, 0) -- +2: initial [byte] len and [int] port
        buf_t[1] = marsh_byte(16) -- size

        local ip = gsub(lower(value), "::", ":0000:")
        local hexadectets = new_tab(8, 0)

        n = 1

        for hdt in gmatch(ip, "[%x]+") do
            -- fill up hexadectets with 0 so all are 4 digits long
            hexadectets[n] = rep("0", 4 - #hdt) .. hdt
            n = n + 1
        end

        -- reset idx var for buf_t

        n = 2

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
        buf_t = new_tab(4 + 2, 0) -- +2: initial [byte] len and [int] port
        buf_t[1] = marsh_byte(4) -- size

        n = 2

        for d in gmatch(value, "(%d+)") do
            buf_t[n] = marsh_byte(d)
            n = n + 1
        end
    end

    return concat(buf_t)
end


local function unmarsh_inet(bytes, version)
    local buf = _M_buf.new_r(version, bytes)
    return _M_buf.read_inet(buf)
end


function _M_buf.write_inet(buf, value)
    _M_buf.write(buf, marsh_inet(value))
end


function _M_buf.read_inet(buf)
    local size = _M_buf.read_byte(buf)
    local bytes = _M_buf.read(buf, size)
    local t_buf = new_tab(size, 0)

    if size == 16 then
        -- ipv6
        local n = 1

        for i = 1, size, 2 do
            t_buf[n] = fmt("%02x", byte(bytes, i)) .. fmt("%02x", byte(bytes, i + 1))
            n = n + 1
        end

        return concat(t_buf, ":")
    end

    -- ipv4
    for i = 1, size do
        t_buf[i] = fmt("%d", byte(bytes, i))
    end

    return concat(t_buf, ".")
end


-- string_list


--[[
local function marsh_string_list(value)

end


local function unmarsh_string_list(bytes)

end
--]]


function _M_buf.write_string_list(buf, value)
    local len = #value

    _M_buf.write_short(buf, len)

    for i = 1, len do
        _M_buf.write_string(buf, value[i])
    end
end


function _M_buf.read_string_list(buf)
    local len = _M_buf.read_short(buf)
    local list = new_tab(len, 0)

    for i = 1, len do
        list[i] = _M_buf.read_string(buf)
    end

    return list
end


-- string_map


--[[
local function marsh_string_map(value)

end


local function unmarsh_string_map(bytes)

end
--]]


function _M_buf.write_string_map(buf, value)
    local len = 0
    local t_buf = _M_buf.new_w(buf.version)

    for k, v in pairs(value) do
        _M_buf.write_string(t_buf, k)
        _M_buf.write_string(t_buf, v)
        len = len + 1
    end

    _M_buf.write_short(buf, len)
    _M_buf.copy(buf, t_buf)
end


function _M_buf.read_string_map(buf)
    local len = _M_buf.read_short(buf)
    local map = new_tab(0, len)

    for _ = 1, len do
        local key = _M_buf.read_string(buf)
        local value = _M_buf.read_string(buf)
        map[key] = value
    end

    return map
end


-- string_multimap


--[[
local function marsh_string_multimap(value)

end


local function unmarsh_string_multimap(bytes)

end
--]]


function _M_buf.write_string_multimap(buf, value)
    local len = 0
    local t_buf = _M_buf.new_w(buf.version)

    for k, v in pairs(value) do
        _M_buf.write_string(t_buf, k)
        _M_buf.write_string_list(t_buf, v)
        len = len + 1
    end

    _M_buf.write_short(buf, len)
    _M_buf.copy(buf, t_buf)
end


function _M_buf.read_string_multimap(buf)
    local len = _M_buf.read_short(buf)
    local multimap = new_tab(0, len)

    for _ = 1, len do
        local key = _M_buf.read_string(buf)
        multimap[key] = _M_buf.read_string_list(buf)
    end

    return multimap
end


-- bytes map


function _M_buf.read_bytes_map(buf)
    local len = _M_buf.read_short(buf)
    local map = new_tab(0, len)

    for i = 1, len do
        local key = _M_buf.read_string(buf)
        map[key] = _M_buf.read_bytes(buf)
    end

    return map
end


-- CQL types definitions
-- @section cql_types_definitions


-- udt_type (definition of a UDT)


function _M_buf.read_udt_type(buf)
    local udt_ks_name = _M_buf.read_string(buf)
    local udt_name = _M_buf.read_string(buf)
    local len = _M_buf.read_short(buf)

    local fields = new_tab(len, 0)

    for i = 1, len do
        fields[i] = {
            name = _M_buf.read_string(buf),
            type = _M_buf.read_option(buf),
        }
    end

    return {
        udt_keyspace = udt_ks_name,
        udt_name     = udt_name,
        fields       = fields,
    }
end


-- tuple_type (definition of a tuple)


function _M_buf.read_tuple_type(buf)
    local len = _M_buf.read_short(buf)

    local fields = new_tab(len, 0)

    for i = 1, len do
        fields[i] = _M_buf.read_option(buf)
    end

    return { fields = fields }
end


-- option


function _M_buf.read_option(buf)
    local id = _M_buf.read_short(buf)
    local value

    if id == cql_types.set or id == cql_types.list then
        value = _M_buf.read_option(buf)

    elseif id == cql_types.map then
        value = { _M_buf.read_option(buf), _M_buf.read_option(buf) }

    elseif id == cql_types.udt then
        value = _M_buf.read_udt_type(buf)

    elseif id == cql_types.tuple then
        value = _M_buf.read_tuple_type(buf)
    end

    -- cql_type_t
    return {
        cql_type = id,
        cql_type_value = value,
    }
end


-- CQL types
-- @section cql_types


-- bytes


local function marsh_cql_raw(value)
    return value
end


local function unmarsh_cql_raw(bytes)
    return bytes
end


-- bigint


local function marsh_cql_bigint(value)
    local first_byte = value >= 0 and 0 or 0xFF

    return char(first_byte, -- only 53 bits from double
    floor(value / 0x1000000000000) % 0x100,
    floor(value / 0x10000000000) % 0x100,
    floor(value / 0x100000000) % 0x100,
    floor(value / 0x1000000) % 0x100,
    floor(value / 0x10000) % 0x100,
    floor(value / 0x100) % 0x100,
    value % 0x100)
end


local function unmarsh_cql_bigint(bytes)
    local b1, b2, b3, b4, b5, b6, b7, b8 = byte(bytes, 1, 8)

    if b1 < 0x80 then
        return ((((((b1 * 0x100 + b2) * 0x100 + b3) * 0x100 + b4)
        * 0x100 + b5) * 0x100 + b6) * 0x100 + b7) * 0x100 + b8
    end

    return ((((((((b1 - 0xFF) * 0x100 + (b2 - 0xFF)) * 0x100 + (b3 - 0xFF))
    * 0x100 + (b4 - 0xFF)) * 0x100 + (b5 - 0xFF)) * 0x100 + (b6 - 0xFF))
    * 0x100 + (b7 - 0xFF)) * 0x100 + (b8 - 0xFF)) - 1
end


-- boolean


local function marsh_cql_boolean(value)
    return marsh_byte(value and 1 or 0)
end


local function unmarsh_cql_boolean(bytes)
    return byte(bytes) == 1
end


-- double


local function marsh_cql_double(value)
    local sign = 0

    if value < 0.0 then
        sign = 0x80
        value = -value
    end

    local mantissa, exponent = frexp(value)

    if mantissa ~= mantissa then
        return char(0xFF, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- nan
    end

    if mantissa == huge then
        if sign == 0 then
            return char(0x7F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- +inf
        end

        return char(0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- -inf
    end

    if mantissa == 0.0 and exponent == 0 then
        return char(sign, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) -- zero
    end

    exponent = exponent + 0x3FE
    mantissa = (mantissa * 2.0 - 1.0) * ldexp(0.5, 53)

    return char(sign + floor(exponent / 0x10),
    (exponent % 0x10) * 0x10 + floor(mantissa / 0x1000000000000),
    floor(mantissa / 0x10000000000) % 0x100,
    floor(mantissa / 0x100000000) % 0x100,
    floor(mantissa / 0x1000000) % 0x100,
    floor(mantissa / 0x10000) % 0x100,
    floor(mantissa / 0x100) % 0x100,
    mantissa % 0x100)
end


local function unmarsh_cql_double(bytes)
    local b1, b2, b3, b4, b5, b6, b7, b8 = byte(bytes, 1, 8)
    local sign = b1 > 0x7F
    local exponent = (b1 % 0x80) * 0x10 + floor(b2 / 0x10)
    local mantissa = ((((((b2 % 0x10) * 0x100 + b3) * 0x100 + b4) * 0x100 + b5)
    * 0x100 + b6) * 0x100 + b7) * 0x100 + b8

    if sign then
        sign = -1

    else
        sign = 1
    end

    if mantissa == 0 and exponent == 0 then
        return sign * 0.0
    end

    if exponent == 0x7FF then
        if mantissa == 0 then
            return sign * huge
        end

        return 0.0/0.0
    end

    return sign * ldexp(1.0 + mantissa / 0x10000000000000, exponent - 0x3FF)
end


-- float


local function marsh_cql_float(value)
    if value == 0 then
        return char(0x00, 0x00, 0x00, 0x00)
    end

    if value ~= value then
        return char(0xFF, 0xFF, 0xFF, 0xFF)
    end

    local sign = 0x00
    if value < 0 then
        sign = 0x80
        value = -value
    end

    local mantissa, exponent = frexp(value)
    exponent = exponent + 0x7F

    if exponent <= 0 then
        mantissa = ldexp(mantissa, exponent - 1)
        exponent = 0

    elseif exponent > 0 then
        if exponent >= 0xFF then
            return char(sign + 0x7F, 0x80, 0x00, 0x00)
        end

        if exponent == 1 then
            exponent = 0

        else
            mantissa = mantissa * 2 - 1
            exponent = exponent - 1
        end
    end

    mantissa = floor(ldexp(mantissa, 23) + 0.5)

    return char(sign + floor(exponent / 2),
    (exponent % 2) * 0x80 + floor(mantissa / 0x10000),
    floor(mantissa / 0x100) % 0x100,
    mantissa % 0x100)
end


local function unmarsh_cql_float(bytes)
    local b1, b2, b3, b4 = byte(bytes, 1, 4)
    local exponent = (b1 % 0x80) * 0x02 + floor(b2 / 0x80)
    local mantissa = ldexp(((b2 % 0x80) * 0x100 + b3) * 0x100 + b4, -23)

    if exponent == 0xFF then
        if mantissa > 0 then
            return 0 / 0
        end

        mantissa = huge
        exponent = 0x7F

    elseif exponent > 0 then
        mantissa = mantissa + 1

    else
        exponent = exponent + 1
    end

    if b1 >= 0x80 then
        mantissa = -mantissa
    end

    return ldexp(mantissa, exponent - 0x7F)
end


-- Nested CQL types
-- @section nested_cql_types


local marsh_cql_value


-- list


local function marsh_cql_list(value, version)
    local len = #value
    local buf_t = new_tab(len + 1, 0)

    if version < 3 then
        buf_t[1] = marsh_short(len)

    else
        buf_t[1] = marsh_int(len)
    end

    for i = 1, len do
        buf_t[i + 1] = marsh_cql_value(value[i], version)
    end

    return concat(buf_t)
end


local function unmarsh_cql_list(bytes, version, cql_type_value)
    local buf = _M_buf.new_r(version, bytes)
    local len

    if version < 3 then
        len = _M_buf.read_short(buf)

    else
        len = _M_buf.read_int(buf)
    end

    local list = new_tab(len, 0)

    for i = 1, len do
        list[i] = _M_buf.read_cql_value(buf, cql_type_value)
    end

    return list
end


-- map


local function marsh_cql_map(value, version)
    local len = 0
    local buf_t = new_tab(4, 0)

    for k, v in pairs(value) do
        buf_t[len + 1] = marsh_cql_value(k, version)
        buf_t[len + 2] = marsh_cql_value(v, version)
        len = len + 2
    end

    if version < 3 then
        insert(buf_t, 1, marsh_short(len / 2))

    else
        insert(buf_t, 1, marsh_int(len / 2))
    end

    return concat(buf_t)
end


local function unmarsh_cql_map(bytes, version, cql_type_value)
    local key_cql_type = cql_type_value[1]
    local value_cql_type = cql_type_value[2]

    if not key_cql_type then
        return error("missing CQL type to unmarshall map keys")
    end

    if not value_cql_type then
        return error("missing CQL type to unmarshall map values")
    end

    local buf = _M_buf.new_r(version, bytes)
    local len

    if version < 3 then
        len = _M_buf.read_short(buf)

    else
        len = _M_buf.read_int(buf)
    end

    local map = new_tab(0, len)

    for _ = 1, len do
        local key = _M_buf.read_cql_value(buf, key_cql_type)
        map[key]  = _M_buf.read_cql_value(buf, value_cql_type)
    end

    return map
end


-- udt


local function marsh_cql_udt(value, version)
    local len = #value
    local buf_t = new_tab(len, 0)

    for i = 1, len do
        buf_t[i] = marsh_cql_value(value[i], version)
    end

    return concat(buf_t)
end


local function unmarsh_cql_udt(bytes, version, cql_type_value)
    local fields = cql_type_value.fields -- see buf_read_udt_type
    local len = #fields
    local udt = new_tab(0, len)
    local buf = _M_buf.new_r(version, bytes)

    for i = 1, len do
        local field = fields[i]
        udt[field.name] = _M_buf.read_cql_value(buf, field.type)
    end

    return udt
end


-- tuple


local marsh_cql_tuple = marsh_cql_udt


local function unmarsh_cql_tuple(bytes, version, cql_type_value)
    local fields = cql_type_value.fields -- see buf_read_tuple_type
    local len = #fields
    local tuple = new_tab(len, 0)
    local buf = _M_buf.new_r(version, bytes)

    for i = 1, len do
        tuple[i] = _M_buf.read_cql_value(buf, fields[i])
    end

    return tuple
end


do
    local cql_marshallers     = {
        -- custom             = 0x00,
        [cql_types.ascii]     = marsh_cql_raw,
        [cql_types.bigint]    = marsh_cql_bigint,
        [cql_types.blob]      = marsh_cql_raw,
        [cql_types.boolean]   = marsh_cql_boolean,
        [cql_types.counter]   = marsh_cql_bigint,
        -- decimal            = 0x06,
        [cql_types.double]    = marsh_cql_double,
        [cql_types.float]     = marsh_cql_float,
        [cql_types.inet]      = marsh_inet,
        [cql_types.int]       = marsh_int,
        [cql_types.text]      = marsh_cql_raw,
        [cql_types.list]      = marsh_cql_list,
        [cql_types.map]       = marsh_cql_map,
        [cql_types.set]       = marsh_cql_list,
        [cql_types.uuid]      = marsh_uuid,
        [cql_types.timestamp] = marsh_cql_bigint,
        [cql_types.varchar]   = marsh_cql_raw,
        [cql_types.varint]    = marsh_int,
        [cql_types.timeuuid]  = marsh_uuid,
        [cql_types.udt]       = marsh_cql_udt,
        [cql_types.tuple]     = marsh_cql_tuple,
    }


    marsh_cql_value = function(value, version)
        if value == CQL_T_UNSET then
            return marsh_unset()
        end

        if value == CQL_T_NULL then
            return marsh_null()
        end

        local cql_t
        local lua_t = type(value)

        if lua_t == "table" then
            -- set by cassandra.uuid() and the likes
            if value.cql_type then
                cql_t = value.cql_type
                value = value.value

            elseif is_list(value) then
                cql_t = cql_types.list

            else
                cql_t = cql_types.map
            end

        elseif lua_t == "number" then
            if floor(value) == value then
                cql_t = cql_types.int

            else
                cql_t = cql_types.float
            end

        elseif lua_t == "boolean" then
            cql_t = cql_types.boolean

        else
            -- default assumed type
            cql_t = cql_types.varchar
        end


        local marshaller = cql_marshallers[cql_t]

        if not marshaller then
            return error(fmt("no marshaller for CQL type 0x%08x", tostring(cql_t)))
        end

        local marshalled = marshaller(value, version)

        if type(marshalled) ~= "string" then
            return error("marshalled value should be a string but is of " ..
                         "type " .. type(marshalled) ..
                         ": " .. tostring(marshalled))
        end

        return marsh_bytes(marshalled)
    end
end


function _M_buf.write_cql_value(buf, value)
    _M_buf.write(buf, marsh_cql_value(value, buf.version))
end


do
    local cql_unmarshallers   = {
        -- custom             = 0x00,
        [cql_types.ascii]     = unmarsh_cql_raw,
        [cql_types.bigint]    = unmarsh_cql_bigint,
        [cql_types.blob]      = unmarsh_cql_raw,
        [cql_types.boolean]   = unmarsh_cql_boolean,
        [cql_types.counter]   = unmarsh_cql_bigint,
        -- decimal            = 0x06,
        [cql_types.double]    = unmarsh_cql_double,
        [cql_types.float]     = unmarsh_cql_float,
        [cql_types.inet]      = unmarsh_inet,
        [cql_types.int]       = unmarsh_int,
        [cql_types.text]      = unmarsh_cql_raw,
        [cql_types.list]      = unmarsh_cql_list,
        [cql_types.map]       = unmarsh_cql_map,
        [cql_types.set]       = unmarsh_cql_list,
        [cql_types.uuid]      = unmarsh_uuid,
        [cql_types.timestamp] = unmarsh_cql_bigint,
        [cql_types.varchar]   = unmarsh_cql_raw,
        [cql_types.varint]    = unmarsh_int,
        [cql_types.timeuuid]  = unmarsh_uuid,
        [cql_types.udt]       = unmarsh_cql_udt,
        [cql_types.tuple]     = unmarsh_cql_tuple,
    }


    function _M_buf.read_cql_value(buf, cql_type_t)
        local unmarshaller = cql_unmarshallers[cql_type_t.cql_type]

        if not unmarshaller then
            if not cql_type_t.cql_type_t then
                return error("no CQL type provided")
            end

            return error(fmt("no unmarshaller for CQL type 0x%08x",
                             cql_type_t.cql_type))
        end

        local bytes = _M_buf.read_bytes(buf)

        if bytes == CQL_T_NULL then
            return
        end

        if type(bytes) ~= "string" then
            return error("expected bytes read from buffer to be a string, " ..
                         "but got type: " .. type(bytes))
        end

        return unmarshaller(bytes, buf.version, cql_type_t.cql_type_value)
    end
end


-- CQL Requests
-- @section cql_requests


local _M_requests = {}


do
    local CQL_VERSION = "3.0.0"

    local QUERY_FLAGS           = {
        VALUES                  = 0x01,
        --SKIP_METADATA         = 0x02,
        PAGE_SIZE               = 0x04,
        WITH_PAGING_STATE       = 0x08,
        WITH_SERIAL_CONSISTENCY = 0x10,
        WITH_DEFAULT_TIMESTAMP  = 0x20,
        WITH_NAMES_FOR_VALUES   = 0x40,
    }


    local function new_request(op_code, body_builder)
        return {
            retries      = 0,
            op_code      = op_code,
            body_builder = body_builder,
        }
    end


    function _M_requests.build_frame(request, protocol_version)
        local header_buf = _M_buf.new_w(protocol_version, 5)
        local body_buf   = _M_buf.new_w(protocol_version)

        request.body_builder(body_buf, request)

        _M_buf.write_byte(header_buf, protocol_version)

        local flags = 0x00
        if request.opts and request.opts.tracing then
            flags = bor(flags, FRAME_FLAGS.TRACING)
        end

        _M_buf.write_byte(header_buf, flags)

        if protocol_version < 3 then
            _M_buf.write_byte(header_buf, 0) -- stream id

        else
            _M_buf.write_short(header_buf, 0) -- stream id
        end

        _M_buf.write_byte(header_buf, request.op_code)
        _M_buf.write_int(header_buf, body_buf.len)

        return _M_buf.get(header_buf) .. _M_buf.get(body_buf)
    end


    local function build_args(body_buf, args, opts)
        local args_buf

        if args or opts then
            args_buf = _M_buf.new_w(body_buf.version, 5)
        end

        if not opts then
            opts = EMPTY_T
        end

        local flags = 0x00
        local consistency = opts.consistency or CONSISTENCIES.one

        -- build args buffer
        if args then
            flags = bor(flags, QUERY_FLAGS.VALUES)

            if body_buf.version >= 3 and opts.named then
                flags = bor(flags, QUERY_FLAGS.WITH_NAMES_FOR_VALUES)

                local n = 0
                local named_args_buf = _M_buf.new_w(body_buf.version)

                for name, val in pairs(args) do
                    n = n + 1
                    _M_buf.write_string(named_args_buf, name)
                    _M_buf.write_cql_value(named_args_buf, val)
                end

                _M_buf.write_short(args_buf, n)
                _M_buf.copy(args_buf, named_args_buf)

            else
                local n = #args

                _M_buf.write_short(args_buf, n)

                for i = 1, n do
                    _M_buf.write_cql_value(args_buf, args[i])
                end
            end
        end

        if opts.page_size then
            flags = bor(flags, QUERY_FLAGS.PAGE_SIZE)
            _M_buf.write_int(args_buf, opts.page_size)
        end

        if opts.paging_state then
            flags = bor(flags, QUERY_FLAGS.WITH_PAGING_STATE)
            _M_buf.write_bytes(opts.paging_state)
        end

        if body_buf.version >= 3 then
            if opts.serial_consistency then
                flags = bor(flags, QUERY_FLAGS.WITH_SERIAL_CONSISTENCY)
                _M_buf.write_short(args_buf, opts.serial_consistency)
            end

            if opts.timestamp then
                flags = bor(flags, QUERY_FLAGS.WITH_DEFAULT_TIMESTAMP)
                _M_buf.write_long(args_buf, opts.timestamp)
            end
        end

        _M_buf.write_short(consistency)
        _M_buf.write_byte(flags)

        if args_buf then
            _M_buf.copy(body_buf, args_buf)
        end
    end


    -- STARTUP


    local function build_startup_body(buf)
        _M_buf.write_string_map(buf, {
            CQL_VERSION = CQL_VERSION
        })
    end


    function _M_requests.startup()
        return new_request(OP_CODES.STARTUP, build_startup_body)
    end


    -- KEYSPACE


    local function build_keyspace_body(buf, request)
        _M_buf.write_long_string(fmt([[USE "%s"]], request.keyspace))
    end


    function _M_requests.keyspace(keyspace)
        local r = new_request(OP_CODES.QUERY, build_keyspace_body)
        r.keyspace = keyspace
        return r
    end


    -- QUERY


    local function build_query_body(buf, request)
        _M_buf.write_long_string(buf, request.query)
        build_args(buf, request.args, request.opts)
    end


    function _M_requests.query(query, args, opts)
        local r = new_request(OP_CODES.QUERY, build_query_body)
        r.query = query
        r.args = args
        r.opts = opts
        return r
    end


    -- PREPARE


    local function build_prepare_body(buf, request)
        _M_buf.write_long_string(buf, request.query)
    end


    function _M_requests.prepare(query)
        local r = new_request(OP_CODES.PREPARE, build_prepare_body)
        r.query = query
        return r
    end


    -- EXECUTE_PREPARE


    local function build_execute_prepared_body(buf, request)
        _M_buf.write_short_bytes(buf, request.query_id)
        build_args(buf, request.args, request.opts)
    end


    function _M_requests.execute_prepared(query_id, args, opts, query)
        local r = new_request(OP_CODES.EXECUTE, build_execute_prepared_body)
        r.query_id = query_id
        r.args = args
        r.opts = opts
        r.query = query -- allow to be re-prepared by cluster
        return r
    end


    -- BATCH


    local function build_batch_body(buf, request)
        local opts = request.opts

        if not opts then
            opts = EMPTY_T
        end

        local n_queries = #request.queries
        local consistency = opts.consistency or CONSISTENCIES.one

        _M_buf.write_byte(buf, request.type)
        _M_buf.write_short(buf, n_queries)

        for i = 1, n_queries do
            local q = request.queries[i] -- {query, args, query_id}

            if opts.prepared then
                _M_buf.write_byte(buf, 1)
                _M_buf.write_short_bytes(buf, q[3])

            else
                _M_buf.write_byte(buf, 0)
                _M_buf.write_long_string(buf, q[1])
            end

            if q[2] then
                local args = q[2]
                local n_args = #args

                -- no support for named args in batch, we reported the issue
                -- at: https://issues.apache.org/jira/browse/CASSANDRA-10246

                _M_buf.write_short(buf, n_args)

                for i = 1, n_args do
                    _M_buf.write_cql_value(buf, args[i])
                end

            else
                _M_buf.write_short(buf, 0)
            end
        end

        _M_buf.write_short(buf, consistency)

        if buf.version >= 3 then
            local flags = 0x00
            local opts_buf = _M_buf.new_w(buf.version)

            if opts.serial_consistency then
                flags = bor(flags, QUERY_FLAGS.WITH_SERIAL_CONSISTENCY)
                _M_buf.write_short(opts_buf, opts.serial_consistency)
            end

            if opts.timestamp then
                flags = bor(flags, QUERY_FLAGS.WITH_DEFAULT_TIMESTAMP)
                _M_buf.write_long(opts_buf, opts.timestamp)
            end

            _M_buf.write_byte(buf, flags)
            _M_buf.copy(buf, opts_buf)
        end
    end


    function _M_requests.batch(queries, opts)
        local r = new_request(OP_CODES.BATCH, build_batch_body)
        r.queries = queries
        r.opts    = opts

        if opts.counter then
            r.type = 2

        elseif opts.logged then
            r.type = 0

        else
            -- unlogged
            r.type = 1
        end

        return r
    end


    -- AUTH_RESPONSE


    local function build_auth_response_body(buf, request)
        _M_buf.write_bytes(buf, request.token)
    end


    function _M_requests.auth_response(token)
        local r = new_request(OP_CODES.AUTH_RESPONSE, build_auth_response_body)
        r.token = token
        return r
    end
end


-- Exports
-- @section exports


local _M        = {
    requests    = _M_requests,
    cql_t_unset = CQL_T_UNSET,
    cql_t_null  = CQL_T_NULL,

    buffer      = _M_buf,
    types       = cql_types,
    is_list     = is_list,
}


return _M
