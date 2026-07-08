#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "tierdb/client.hpp"

namespace duckdb {

class TierDBCatalog;

class TierDBTransaction : public Transaction {
public:
	TierDBTransaction(TierDBCatalog &tierdb_catalog, TransactionManager &manager, ClientContext &context);
	~TierDBTransaction() override;

	static TierDBTransaction &Get(ClientContext &context, Catalog &catalog);

	// Returns the merged read for a table, acquiring and caching a read pin on
	// first touch so every scan in this transaction reads the same frozen (T, S).
	string GetPinnedScan(TierDBClient &client, const TierDBTableSchema &schema);

	// Releases every pin this transaction acquired. Called at commit and rollback.
	void ReleasePins(TierDBClient &client);

	// Opens the Postgres write transaction on first write, so every chunk and
	// statement of this DuckDB transaction lands atomically. Idempotent.
	void EnsureWriteTxn(TierDBClient &client);

	// Commits or rolls back the write transaction at the DuckDB boundary. Both
	// are no-ops for a read-only transaction that never wrote.
	void CommitWrite(TierDBClient &client);
	void RollbackWrite(TierDBClient &client);

private:
	mutex pin_lock;
	unordered_map<int64_t, string> scan_by_table;
	vector<int64_t> pins;
	bool write_active = false;
};

} // namespace duckdb
