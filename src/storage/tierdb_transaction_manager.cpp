#include "storage/tierdb_transaction_manager.hpp"

namespace duckdb {

TierDBTransactionManager::TierDBTransactionManager(AttachedDatabase &db, TierDBCatalog &tierdb_catalog)
    : TransactionManager(db), tierdb_catalog(tierdb_catalog) {
}

Transaction &TierDBTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<TierDBTransaction>(tierdb_catalog, *this, context);
	auto &result = *transaction;
	lock_guard<mutex> guard(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData TierDBTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &tierdb_transaction = transaction.Cast<TierDBTransaction>();
	ErrorData error;
	try {
		// Commit the write transaction first: if the heap/delta commit fails the
		// whole DuckDB transaction must report failure, not silently drop writes.
		tierdb_transaction.CommitWrite(tierdb_catalog.GetClient());
	} catch (std::exception &ex) {
		error = ErrorData(ex);
		try {
			tierdb_transaction.RollbackWrite(tierdb_catalog.GetClient());
		} catch (std::exception &) {
			// Best effort: the connection state resets on the next transaction.
		}
	}
	try {
		tierdb_transaction.ReleasePins(tierdb_catalog.GetClient());
	} catch (std::exception &ex) {
		if (!error.HasError()) {
			error = ErrorData(ex);
		}
	}
	lock_guard<mutex> guard(transaction_lock);
	transactions.erase(transaction);
	return error;
}

void TierDBTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &tierdb_transaction = transaction.Cast<TierDBTransaction>();
	try {
		tierdb_transaction.RollbackWrite(tierdb_catalog.GetClient());
	} catch (std::exception &) {
		// Best effort: the connection state resets on the next transaction.
	}
	try {
		tierdb_transaction.ReleasePins(tierdb_catalog.GetClient());
	} catch (std::exception &) {
		// Best effort: the pin rows carry a TTL and will expire on their own.
	}
	lock_guard<mutex> guard(transaction_lock);
	transactions.erase(transaction);
}

void TierDBTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

} // namespace duckdb
