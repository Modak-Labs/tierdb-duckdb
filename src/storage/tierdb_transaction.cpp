#include "storage/tierdb_transaction.hpp"
#include "storage/tierdb_catalog.hpp"

namespace duckdb {

TierDBTransaction::TierDBTransaction(TierDBCatalog &tierdb_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
}

TierDBTransaction::~TierDBTransaction() = default;

TierDBTransaction &TierDBTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<TierDBTransaction>();
}

string TierDBTransaction::GetPinnedScan(TierDBClient &client, const TierDBTableSchema &schema) {
	lock_guard<mutex> guard(pin_lock);
	auto it = scan_by_table.find(schema.table_id);
	if (it != scan_by_table.end()) {
		return it->second;
	}
	auto pinned = client.AcquireReadScan(schema.schema_name, schema.table_name);
	if (pinned.has_pin) {
		pins.push_back(pinned.pin_id);
	}
	scan_by_table.emplace(schema.table_id, pinned.scan_sql);
	return pinned.scan_sql;
}

void TierDBTransaction::ReleasePins(TierDBClient &client) {
	vector<int64_t> to_release;
	{
		lock_guard<mutex> guard(pin_lock);
		to_release = std::move(pins);
		pins.clear();
		scan_by_table.clear();
	}
	for (auto pin_id : to_release) {
		client.ReleaseReadPin(pin_id);
	}
}

void TierDBTransaction::EnsureWriteTxn(TierDBClient &client) {
	if (write_active) {
		return;
	}
	client.TxnBegin();
	write_active = true;
}

void TierDBTransaction::CommitWrite(TierDBClient &client) {
	if (!write_active) {
		return;
	}
	write_active = false;
	client.TxnCommit();
}

void TierDBTransaction::RollbackWrite(TierDBClient &client) {
	if (!write_active) {
		return;
	}
	write_active = false;
	client.TxnRollback();
}

} // namespace duckdb
