LATEST_VERSION = 0
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4
MAX_BASE_PAGES = 16
METADATA_COLUMNS = 5 # indirection, rid, time, schema, baserid
ENTRY_SIZE = 8 # 8 bytes
CAPACITY = 4096
MAX_BASE_PAGES = 16
SHARED = 0 # lock value for shared lock
EXCLUSIVE = 1 # lock value for exclusive lock
INDEX = "index" # tells lock manager we are locking an index
RECORD = "record" # tells lock manager we are locking a record

LOGICAL_ERROR = 500

# query function names for checks later
DELETE = "delete"
INSERT = "insert"
SELECT = "select"
SELECT_VERSION = "select_version"
UPDATE = "update"
SUM = "sum"
SUM_VERSION = "sum_version"
INCREMENT = "increment"