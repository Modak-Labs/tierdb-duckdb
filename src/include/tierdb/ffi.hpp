#pragma once

#include <cstdint>

extern "C" {

typedef struct TierDBConn TierDBConn;

struct TierDBConnResult {
	char *err;
	TierDBConn *value;
};

struct TierDBStringResult {
	char *err;
	char *value;
};

struct TierDBStatus {
	char *err;
};

TierDBConnResult tierdb_connect(const char *dsn);
void tierdb_conn_free(TierDBConn *conn);

TierDBStringResult tierdb_table_schema(TierDBConn *conn, const char *schema, const char *table);
// Returns {"pin_id": i64|null, "scan_sql": "..", "attach_sql": ".."|null}.
TierDBStringResult tierdb_acquire_read_scan(TierDBConn *conn, const char *schema, const char *table);
TierDBStatus tierdb_release_read_pin(TierDBConn *conn, int64_t pin_id);
TierDBStatus tierdb_txn_begin(TierDBConn *conn);
TierDBStatus tierdb_txn_commit(TierDBConn *conn);
TierDBStatus tierdb_txn_rollback(TierDBConn *conn);
TierDBStringResult tierdb_list_tables(TierDBConn *conn);
// The chunk functions return {"count": i64, "attach_sql": ".."|null,
// "lake_sql": [".."]}; attach_sql then lake_sql run on DuckDB before commit.
TierDBStringResult tierdb_insert_chunk(TierDBConn *conn, const char *schema, const char *table,
                                       const char *rows_json);
TierDBStringResult tierdb_delete_chunk(TierDBConn *conn, const char *schema, const char *table,
                                       const char *rows_json);
TierDBStringResult tierdb_update_chunk(TierDBConn *conn, const char *schema, const char *table,
                                       const char *rows_json);

void tierdb_free_string(char *s);

} // extern "C"
