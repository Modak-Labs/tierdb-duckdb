#include "storage/tierdb_insert.hpp"
#include "storage/tierdb_catalog.hpp"
#include "storage/tierdb_table.hpp"
#include "storage/tierdb_transaction.hpp"
#include "storage/tierdb_json.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "yyjson.hpp"

#include <cstdlib>

using namespace duckdb_yyjson;

namespace duckdb {

PhysicalTierDBInsert::PhysicalTierDBInsert(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                           TierDBTableEntry &table_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table(table_p) {
	for (auto &col : table.GetColumns().Physical()) {
		column_names.push_back(col.Name());
	}
}

class TierDBInsertGlobalState : public GlobalSinkState {
public:
	mutex lock;
	idx_t insert_count = 0;
};

unique_ptr<GlobalSinkState> PhysicalTierDBInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<TierDBInsertGlobalState>();
}

static string ChunkToJsonArray(DataChunk &chunk, const vector<string> &column_names) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto root = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, root);

	auto col_count = chunk.ColumnCount();
	for (idx_t row = 0; row < chunk.size(); row++) {
		auto obj = yyjson_mut_obj(doc);
		for (idx_t col = 0; col < col_count; col++) {
			TierDBAddCell(doc, obj, column_names[col].c_str(), chunk.GetValue(col, row));
		}
		yyjson_mut_arr_append(root, obj);
	}

	char *json = yyjson_mut_write(doc, 0, nullptr);
	string result = json ? string(json) : string("[]");
	if (json) {
		free(json);
	}
	yyjson_mut_doc_free(doc);
	return result;
}

SinkResultType PhysicalTierDBInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	chunk.Flatten();

	auto rows_json = ChunkToJsonArray(chunk, column_names);
	auto &client = table.catalog.Cast<TierDBCatalog>().GetClient();
	TierDBTransaction::Get(context.client, table.catalog).EnsureWriteTxn(client);
	auto result = client.InsertChunk(table.tierdb_schema.schema_name, table.tierdb_schema.table_name, rows_json);
	TierDBExecuteLakeDml(context.client, result);

	auto &gstate = input.global_state.Cast<TierDBInsertGlobalState>();
	lock_guard<mutex> guard(gstate.lock);
	gstate.insert_count += NumericCast<idx_t>(result.count);
	return SinkResultType::NEED_MORE_INPUT;
}

SourceResultType PhysicalTierDBInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<TierDBInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.insert_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
