local utils = require "cassandra.utils"
local constants_v2 = require "cassandra.constants.constants_v2"

local constants_v3 = utils.deep_copy(constants_v2)

constants_v3.version_codes.REQUEST = 0x03
constants_v3.version_codes.RESPONSE = 0x83

constants_v3.query_flags.DEFAULT_TIMESTAMP = 0x20
constants_v3.query_flags.NAMED_VALUES = 0x40

constants_v3.rows_flags.udt = 0x30
constants_v3.rows_flags.tuple = 0x31

return constants_v3
