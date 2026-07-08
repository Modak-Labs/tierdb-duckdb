#include "storage/tierdb_table.hpp"
#include "storage/tierdb_catalog.hpp"
#include "storage/tierdb_transaction.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/common/index_map.hpp"

namespace duckdb {

TierDBTableEntry::TierDBTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                   TierDBTableSchema tierdb_schema_p)
    : TableCatalogEntry(catalog, schema, info), tierdb_schema(std::move(tierdb_schema_p)) {
}

unique_ptr<BaseStatistics> TierDBTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

struct TierDBScanBindData : public TableFunctionData {
	string sql;
	vector<string> column_names;
	// Routing virtual columns (see TierDBTableEntry::RoutingColumns) map back to
	// the real heap column the merged read must project for them.
	unordered_map<column_t, string> virtual_to_name;
	// The owning table, so LogicalGet::GetTable() resolves and DELETE/UPDATE bind.
	TableCatalogEntry *table = nullptr;
};

static BindInfo TierDBScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TierDBScanBindData>();
	return BindInfo(*bind_data.table);
}

struct TierDBScanGlobalState : public GlobalTableFunctionState {
	unique_ptr<Connection> connection;
	unique_ptr<QueryResult> result;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static string QuoteIdent(const string &name) {
	string out = "\"";
	for (auto c : name) {
		if (c == '"') {
			out += '"';
		}
		out += c;
	}
	out += '"';
	return out;
}

// Projects the merged read to the requested columns. A routing virtual column
// reads its real heap column; any other virtual column (e.g. count(*)) maps to
// a constant since only row presence matters.
static string ProjectedScanSql(const TierDBScanBindData &bind_data, const vector<column_t> &column_ids) {
	string projection;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (i > 0) {
			projection += ", ";
		}
		auto col_id = column_ids[i];
		if (col_id < bind_data.column_names.size()) {
			projection += QuoteIdent(bind_data.column_names[col_id]);
			continue;
		}
		auto routed = bind_data.virtual_to_name.find(col_id);
		if (routed != bind_data.virtual_to_name.end()) {
			projection += QuoteIdent(routed->second);
		} else {
			projection += "CAST(1 AS BIGINT)";
		}
	}
	if (projection.empty()) {
		projection = "CAST(1 AS BIGINT)";
	}
	return "SELECT " + projection + " FROM (" + bind_data.sql + ") tierdb_merged";
}

static unique_ptr<GlobalTableFunctionState> TierDBScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<TierDBScanBindData>();
	auto state = make_uniq<TierDBScanGlobalState>();
	state->connection = make_uniq<Connection>(*context.db);
	auto result = state->connection->Query(ProjectedScanSql(bind_data, input.column_ids));
	if (result->HasError()) {
		throw IOException("tierdb: merged read failed: %s", result->GetError());
	}
	state->result = std::move(result);
	return std::move(state);
}

static void TierDBScanExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<TierDBScanGlobalState>();
	auto chunk = state.result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return;
	}
	output.Move(*chunk);
}

TableFunction TierDBTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto &tierdb_catalog = catalog.Cast<TierDBCatalog>();
	auto &transaction = TierDBTransaction::Get(context, catalog);

	auto data = make_uniq<TierDBScanBindData>();
	data->sql = transaction.GetPinnedScan(tierdb_catalog.GetClient(), tierdb_schema);
	for (auto &col : GetColumns().Physical()) {
		data->column_names.push_back(col.Name());
	}
	for (auto &rc : RoutingColumns()) {
		data->virtual_to_name[rc.virtual_id] = rc.name;
	}
	data->table = this;
	bind_data = std::move(data);

	TableFunction function("tierdb_scan", {}, TierDBScanExecute, nullptr, TierDBScanInitGlobal);
	function.projection_pushdown = true;
	function.get_bind_info = TierDBScanGetBindInfo;
	return function;
}

TableStorageInfo TierDBTableEntry::GetStorageInfo(ClientContext &context) {
	return TableStorageInfo();
}

vector<TierDBRoutingColumn> TierDBTableEntry::RoutingColumns() const {
	vector<TierDBRoutingColumn> out;
	auto add = [&](const string &name) {
		LogicalType type = LogicalType::VARCHAR;
		for (auto &c : tierdb_schema.columns) {
			if (c.name == name) {
				type = TierDBLogicalTypeFromString(c.duckdb_type);
				break;
			}
		}
		column_t id = VIRTUAL_COLUMN_START + 1 + out.size();
		out.push_back(TierDBRoutingColumn {id, name, type});
	};
	for (auto &pk : tierdb_schema.primary_key_cols) {
		add(pk);
	}
	add(tierdb_schema.tier_key_col);
	return out;
}

virtual_column_map_t TierDBTableEntry::GetVirtualColumns() const {
	virtual_column_map_t virtual_columns;
	virtual_columns.insert(make_pair(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::ROW_TYPE)));
	idx_t k = 0;
	for (auto &rc : RoutingColumns()) {
		virtual_columns.insert(make_pair(rc.virtual_id, TableColumn("__tierdb_route_" + std::to_string(k++), rc.type)));
	}
	return virtual_columns;
}

vector<column_t> TierDBTableEntry::GetRowIdColumns() const {
	vector<column_t> ids;
	for (auto &rc : RoutingColumns()) {
		ids.push_back(rc.virtual_id);
	}
	return ids;
}

void TierDBTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                             LogicalUpdate &update, ClientContext &context) {
	update.update_is_del_and_insert = true;
	physical_index_set_t all_columns;
	for (auto &column : GetColumns().Physical()) {
		all_columns.insert(column.Physical());
	}
	LogicalUpdate::BindExtraColumns(*this, get, proj, update, all_columns);
}

} // namespace duckdb
