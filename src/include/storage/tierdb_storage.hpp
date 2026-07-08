#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class TierDBStorageExtension : public StorageExtension {
public:
	TierDBStorageExtension();
};

} // namespace duckdb
