local error_codes = {
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

return {
  version_codes = {
    REQUEST=0x02,
    RESPONSE=0x82
  },
  flags = {
    COMPRESSION=0x01, -- not implemented
    TRACING=0x02
  },
  op_codes = {
    ERROR=0x00,
    STARTUP=0x01,
    READY=0x02,
    AUTHENTICATE=0x03,
    -- 0x04
    OPTIONS=0x05,
    SUPPORTED=0x06,
    QUERY=0x07,
    RESULT=0x08,
    PREPARE=0x09,
    EXECUTE=0x0A,
    REGISTER=0x0B,
    EVENT=0x0C,
    BATCH=0x0D,
    AUTH_CHALLENGE=0x0E,
    AUTH_RESPONSE=0x0F,
    AUTH_SUCCESS=0x10,
  },
  batch_types = {
    LOGGED=0,
    UNLOGGED=1,
    COUNTER=2
  },
  query_flags = {
    VALUES=0x01,
    SKIP_METADATA=0x02, -- not implemented
    PAGE_SIZE=0x04,
    PAGING_STATE=0x08,
    -- 0x09
    SERIAL_CONSISTENCY=0x10
  },
  consistency = {
    ANY=0x0000,
    ONE=0x0001,
    TWO=0x0002,
    THREE=0x0003,
    QUORUM=0x0004,
    ALL=0x0005,
    LOCAL_QUORUM=0x0006,
    EACH_QUORUM=0x0007,
    SERIAL=0x0008,
    LOCAL_SERIAL=0x0009,
    LOCAL_ONE=0x000A
  },
  result_kinds = {
    VOID=0x01,
    ROWS=0x02,
    SET_KEYSPACE=0x03,
    PREPARED=0x04,
    SCHEMA_CHANGE=0x05
  },
  rows_flags = {
    GLOBAL_TABLES_SPEC=0x01,
    HAS_MORE_PAGES=0x02,
    -- 0x03
    NO_METADATA=0x04
  },
  error_codes = error_codes,
  error_codes_translation = {
    [error_codes.SERVER]="Server error",
    [error_codes.PROTOCOL]="Protocol error",
    [error_codes.BAD_CREDENTIALS]="Bad credentials",
    [error_codes.UNAVAILABLE_EXCEPTION]="Unavailable exception",
    [error_codes.OVERLOADED]="Overloaded",
    [error_codes.IS_BOOTSTRAPPING]="Is_bootstrapping",
    [error_codes.TRUNCATE_ERROR]="Truncate_error",
    [error_codes.WRITE_TIMEOUT]="Write_timeout",
    [error_codes.READ_TIMEOUT]="Read_timeout",
    [error_codes.SYNTAX_ERROR]="Syntax_error",
    [error_codes.UNAUTHORIZED]="Unauthorized",
    [error_codes.INVALID]="Invalid",
    [error_codes.CONFIG_ERROR]="Config_error",
    [error_codes.ALREADY_EXISTS]="Already_exists",
    [error_codes.UNPREPARED]="Unprepared"
  },
}
