local QUERY_FLAGS = {
  COMPRESSION = 0x01, -- not implemented
  TRACING = 0x02
}

local OP_CODES = {
  ERROR = 0x00,
  STARTUP = 0x01,
  READY = 0x02,
  AUTHENTICATE = 0x03,
  OPTIONS = 0x05,
  SUPPORTED = 0x06,
  QUERY = 0x07,
  RESULT = 0x08,
  PREPARE = 0x09,
  EXECUTE = 0x0A,
  REGISTER = 0x0B,
  EVENT = 0x0C,
  BATCH = 0x0D,
  AUTH_CHALLENGE = 0x0E,
  AUTH_RESPONSE = 0x0F,
  AUTH_SUCCESS = 0x10
}

local cql_types = {
  custom    = 0x00,
  ascii     = 0x01,
  bigint    = 0x02,
  blob      = 0x03,
  boolean   = 0x04,
  counter   = 0x05,
  decimal   = 0x06,
  double    = 0x07,
  float     = 0x08,
  int       = 0x09,
  text      = 0x0A,
  timestamp = 0x0B,
  uuid      = 0x0C,
  varchar   = 0x0D,
  varint    = 0x0E,
  timeuuid  = 0x0F,
  inet      = 0x10,
  list      = 0x20,
  map       = 0x21,
  set       = 0x22,
  udt       = 0x30,
  tuple     = 0x31
}

local consistencies = {
  any = 0x0000,
  one = 0x0001,
  two = 0x0002,
  three = 0x0003,
  quorum = 0x0004,
  all = 0x0005,
  local_quorum = 0x0006,
  each_quorum = 0x0007,
  serial = 0x0008,
  local_serial = 0x0009,
  local_one = 0x000a
}

local ERRORS = {
  SERVER = 0x0000,
  PROTOCOL = 0x000A,
  BAD_CREDENTIALS = 0x0100,
  UNAVAILABLE_EXCEPTION = 0x1000,
  OVERLOADED = 0x1001,
  IS_BOOTSTRAPPING = 0x1002,
  TRUNCATE_ERROR = 0x1003,
  WRITE_TIMEOUT = 0x1100,
  READ_TIMEOUT = 0x1200,
  SYNTAX_ERROR = 0x2000,
  UNAUTHORIZED = 0x2100,
  INVALID = 0x2200,
  CONFIG_ERROR = 0x2300,
  ALREADY_EXISTS = 0x2400,
  UNPREPARED = 0x2500
}

local ERRORS_TRANSLATIONS = {
  [ERRORS.SERVER] = "Server error",
  [ERRORS.PROTOCOL] = "Protocol error",
  [ERRORS.BAD_CREDENTIALS] = "Bad credentials",
  [ERRORS.UNAVAILABLE_EXCEPTION] = "Unavailable exception",
  [ERRORS.OVERLOADED] = "Overloaded",
  [ERRORS.IS_BOOTSTRAPPING] = "Is bootstrapping",
  [ERRORS.TRUNCATE_ERROR] = "Truncate error",
  [ERRORS.WRITE_TIMEOUT] = "Write timeout",
  [ERRORS.READ_TIMEOUT] = "Read timeout",
  [ERRORS.SYNTAX_ERROR] = "Syntax error",
  [ERRORS.UNAUTHORIZED] = "Unauthorized",
  [ERRORS.INVALID] = "Invalid",
  [ERRORS.CONFIG_ERROR] = "Config error",
  [ERRORS.ALREADY_EXISTS] = "Already exists",
  [ERRORS.UNPREPARED] = "Unprepared"
}

return {
  -- public
  consistencies = consistencies,
  -- private
  cql_types = cql_types,
  QUERY_FLAGS = QUERY_FLAGS,
  OP_CODES = OP_CODES,
  ERRORS = ERRORS,
  ERRORS_TRANSLATIONS = ERRORS_TRANSLATIONS
}
