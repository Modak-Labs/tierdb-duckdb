#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "tierdb/client.hpp"

namespace duckdb {

// One column that identifies and routes a row for DML: its virtual id (what the
// binder projects as a "row id"), the real heap column it reads from, and that
// column's type.
struct TierDBRoutingColumn {
	column_t virtual_id;
	string name;
	LogicalType type;
};

class TierDBTableEntry : public TableCatalogEntry {
public:
	TierDBTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                 TierDBTableSchema tierdb_schema);

	TierDBTableSchema tierdb_schema;

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;

	// The delete/update path locates a row by its primary key and tier key
	// instead of a physical row id, so these become the table's row-id columns.
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;
	vector<TierDBRoutingColumn> RoutingColumns() const;

	// Updates always run as delete + insert so the whole new row image is
	// available to route and to write into the lake overlay.
	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;
};

} // namespace duckdb
