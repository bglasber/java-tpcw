namespace java com.dynamic.mastering //

typedef i32 clientid

enum exec_status_type
{
    PGRES_EMPTY_QUERY = 0,      /* empty query string was executed */
    PGRES_COMMAND_OK,           /* a query command that doesn't return
                                 * anything was executed properly by the
                                 * backend */
    PGRES_TUPLES_OK,            /* a query command that returns tuples was
                                 * executed properly by the backend, PGresult
                                 * contains the result tuples */
    PGRES_COPY_OUT,             /* Copy Out data transfer in progress */
    PGRES_COPY_IN,              /* Copy In data transfer in progress */
    PGRES_BAD_RESPONSE,         /* an unexpected response was recv'd from the
                                 * backend */
    PGRES_NONFATAL_ERROR,       /* notice or warning message */
    PGRES_FATAL_ERROR,          /* query failed */
    PGRES_COPY_BOTH,            /* Copy In/Out data transfer in progress */
    PGRES_SINGLE_TUPLE,         /* single tuple from larger resultset */
    CONN_MISSING_ERROR,         /* there should be connection in the map for a given id, but it is missing*/
    CONN_NOT_AVAILABLE,         /* At the moment, there is no connection available in the conn pool */
    INVALID_SVV,                /* A malformed svv was passed for the transaction */
    ERROR_WITH_WRITE_SET,
    MASTER_CHANGE_OK,
    MASTER_CHANGE_ERROR,
    INVALID_SERIALIZATION,      /* the serialization string sent via rpc_stored_procedure is invalid */
    FUNCTION_NOT_FOUND,          /* the function name with argTypes was not found */
    GENERIC_ERROR,
}

struct primary_key {
    1:required i32 table_id,
    2:required i64 row_id
}

struct bucket_key {
    1:required i32 map_key,
    2:required i64 range_key
}

typedef list<i32> site_version_vector

struct partition_to_primary_key {
    1: list<i32> present_partitions,
    2: map<i32, list<primary_key>> pks_per_partition,
}

const byte CTT_LOCK = 84; /* T */
const byte CDT_LOCK = 68; /* D */
const byte CMT_LOCK = 77; /* M */
const byte OPEN_LOCK = 79; /* O */

struct ss_db_value {
  1: byte lock_value,
  2: i16 counter,
  3: clientid id,
  4: i32 site,
}
