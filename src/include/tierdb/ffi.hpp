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
TierDBStringResult tierdb_acquire_read_scan(TierDBConn *conn, const char *schema, const char *table);
TierDBStatus tierdb_release_read_pin(TierDBConn *conn, int64_t pin_id);
TierDBStatus tierdb_txn_begin(TierDBConn *conn);
TierDBStatus tierdb_txn_commit(TierDBConn *conn);
TierDBStatus tierdb_txn_rollback(TierDBConn *conn);
TierDBStringResult tierdb_list_tables(TierDBConn *conn);
TierDBStringResult tierdb_insert_chunk(TierDBConn *conn, const char *schema, const char *table,
                                       const char *rows_json);
TierDBStringResult tierdb_delete_chunk(TierDBConn *conn, const char *schema, const char *table,
                                       const char *rows_json);
TierDBStringResult tierdb_update_chunk(TierDBConn *conn, const char *schema, const char *table,
                                       const char *rows_json);

void tierdb_free_string(char *s);

} // extern "C"
