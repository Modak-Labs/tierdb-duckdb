#include "storage/tierdb_update.hpp"
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

PhysicalTierDBUpdate::PhysicalTierDBUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                           TierDBTableEntry &table_p, vector<idx_t> new_value_indexes_p,
                                           vector<string> new_col_names_p, vector<string> old_key_names_p,
                                           idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table(table_p), new_value_indexes(std::move(new_value_indexes_p)), new_col_names(std::move(new_col_names_p)),
      old_key_names(std::move(old_key_names_p)) {
}

class TierDBUpdateGlobalState : public GlobalSinkState {
public:
	mutex lock;
	idx_t update_count = 0;
};

unique_ptr<GlobalSinkState> PhysicalTierDBUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<TierDBUpdateGlobalState>();
}

SinkResultType PhysicalTierDBUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	chunk.Flatten();

	// The old routing key columns are the trailing columns of the child chunk,
	// in the same order the table declared its row-id columns.
	auto key_count = old_key_names.size();
	auto old_base = chunk.ColumnCount() - key_count;

	auto doc = yyjson_mut_doc_new(nullptr);
	auto root = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, root);
	for (idx_t row = 0; row < chunk.size(); row++) {
		auto pair = yyjson_mut_obj(doc);

		auto old_obj = yyjson_mut_obj(doc);
		for (idx_t k = 0; k < key_count; k++) {
			TierDBAddCell(doc, old_obj, old_key_names[k].c_str(), chunk.GetValue(old_base + k, row));
		}
		yyjson_mut_obj_add_val(doc, pair, "old", old_obj);

		auto new_obj = yyjson_mut_obj(doc);
		for (idx_t c = 0; c < new_col_names.size(); c++) {
			TierDBAddCell(doc, new_obj, new_col_names[c].c_str(), chunk.GetValue(new_value_indexes[c], row));
		}
		yyjson_mut_obj_add_val(doc, pair, "new", new_obj);

		yyjson_mut_arr_append(root, pair);
	}
	char *json = yyjson_mut_write(doc, 0, nullptr);
	string rows_json = json ? string(json) : string("[]");
	if (json) {
		free(json);
	}
	yyjson_mut_doc_free(doc);

	auto &client = table.catalog.Cast<TierDBCatalog>().GetClient();
	TierDBTransaction::Get(context.client, table.catalog).EnsureWriteTxn(client);
	auto changed = client.UpdateChunk(table.tierdb_schema.schema_name, table.tierdb_schema.table_name, rows_json);

	auto &gstate = input.global_state.Cast<TierDBUpdateGlobalState>();
	lock_guard<mutex> guard(gstate.lock);
	gstate.update_count += NumericCast<idx_t>(changed);
	return SinkResultType::NEED_MORE_INPUT;
}

SourceResultType PhysicalTierDBUpdate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<TierDBUpdateGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.update_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
