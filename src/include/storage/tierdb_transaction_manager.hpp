#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/reference_map.hpp"
#include "storage/tierdb_catalog.hpp"
#include "storage/tierdb_transaction.hpp"

namespace duckdb {

class TierDBTransactionManager : public TransactionManager {
public:
	TierDBTransactionManager(AttachedDatabase &db, TierDBCatalog &tierdb_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	TierDBCatalog &tierdb_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<TierDBTransaction>> transactions;
};

} // namespace duckdb
