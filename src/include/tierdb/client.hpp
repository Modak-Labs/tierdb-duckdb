#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/mutex.hpp"

struct TierDBConn;

namespace duckdb {

struct TierDBColumn {
	string name;
	string pg_type;
	// Canonical DuckDB type string from tierdb-core, e.g. "DECIMAL(12,3)".
	string duckdb_type;
};

struct TierDBTableSchema {
	int64_t table_id;
	string schema_name;
	string table_name;
	string lake_format;
	string lake_table_ref;
	string tier_key_col;
	string tier_key_type;
	vector<string> primary_key_cols;
	vector<TierDBColumn> columns;
};

struct TierDBTableRef {
	string schema_name;
	string table_name;
};

struct TierDBPinnedScan {
	bool has_pin;
	int64_t pin_id;
	string scan_sql;
};

class TierDBClient {
public:
	explicit TierDBClient(const string &dsn);
	~TierDBClient();
	TierDBClient(const TierDBClient &) = delete;
	TierDBClient &operator=(const TierDBClient &) = delete;

	vector<TierDBTableRef> ListTables();
	bool TryGetTableSchema(const string &schema, const string &table, TierDBTableSchema &result);
	TierDBPinnedScan AcquireReadScan(const string &schema, const string &table);
	void ReleaseReadPin(int64_t pin_id);
	void TxnBegin();
	void TxnCommit();
	void TxnRollback();
	int64_t InsertChunk(const string &schema, const string &table, const string &rows_json);
	int64_t DeleteChunk(const string &schema, const string &table, const string &rows_json);
	int64_t UpdateChunk(const string &schema, const string &table, const string &rows_json);

private:
	::TierDBConn *conn;
	mutex conn_lock;
};

LogicalType TierDBLogicalTypeFromString(const string &duckdb_type);

} // namespace duckdb
