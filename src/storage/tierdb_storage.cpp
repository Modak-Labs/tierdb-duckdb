#include "storage/tierdb_storage.hpp"
#include "storage/tierdb_catalog.hpp"
#include "storage/tierdb_transaction_manager.hpp"

#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/common/exception/binder_exception.hpp"

namespace duckdb {

static unique_ptr<Catalog> TierDBAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                        AttachedDatabase &db, const string &name, AttachInfo &info,
                                        AttachOptions &options) {
	string dsn = info.path;
	for (auto &entry : options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "dsn") {
			dsn = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for tierdb attach: %s", entry.first);
		}
	}
	if (dsn.empty()) {
		throw BinderException("tierdb attach requires a libpq connection string (ATTACH '<dsn>' AS name (TYPE tierdb))");
	}
	return make_uniq<TierDBCatalog>(db, std::move(dsn), options.access_mode);
}

static unique_ptr<TransactionManager> TierDBCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                     AttachedDatabase &db, Catalog &catalog) {
	auto &tierdb_catalog = catalog.Cast<TierDBCatalog>();
	return make_uniq<TierDBTransactionManager>(db, tierdb_catalog);
}

TierDBStorageExtension::TierDBStorageExtension() {
	attach = TierDBAttach;
	create_transaction_manager = TierDBCreateTransactionManager;
}

} // namespace duckdb
