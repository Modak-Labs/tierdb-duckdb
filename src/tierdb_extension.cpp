#include "tierdb_extension.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "storage/tierdb_storage.hpp"

namespace duckdb {

void TierdbExtension::Load(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	StorageExtension::Register(config, "tierdb", make_shared_ptr<TierDBStorageExtension>());

	// A tierdb scan reads the hot heap and delta through postgres_scan, the cold
	// lake through iceberg_scan, and unpacks delta payloads with the json
	// functions. tierdb owns those peers on the user's behalf so a plain SELECT
	// over an attached tierdb database works without the user loading anything.
	config.SetOptionByName("autoinstall_known_extensions", Value::BOOLEAN(true));
	config.SetOptionByName("autoload_known_extensions", Value::BOOLEAN(true));
	for (auto &peer : {"json", "postgres_scanner", "iceberg"}) {
		ExtensionHelper::TryAutoLoadExtension(db, peer);
	}
}

string TierdbExtension::Name() {
	return "tierdb";
}

string TierdbExtension::Version() const {
	return EXT_VERSION_TIERDB;
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(tierdb, loader) {
	duckdb::TierdbExtension().Load(loader);
}
}
