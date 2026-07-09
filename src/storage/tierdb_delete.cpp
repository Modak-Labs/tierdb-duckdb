#include "storage/tierdb_delete.hpp"
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

PhysicalTierDBDelete::PhysicalTierDBDelete(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                           TierDBTableEntry &table_p, vector<idx_t> key_indexes_p,
                                           vector<string> key_names_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table(table_p), key_indexes(std::move(key_indexes_p)), key_names(std::move(key_names_p)) {
}

class TierDBDeleteGlobalState : public GlobalSinkState {
public:
	mutex lock;
	idx_t delete_count = 0;
};

unique_ptr<GlobalSinkState> PhysicalTierDBDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<TierDBDeleteGlobalState>();
}

static string KeysToJsonArray(DataChunk &chunk, const vector<idx_t> &key_indexes, const vector<string> &key_names) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto root = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, root);

	for (idx_t row = 0; row < chunk.size(); row++) {
		auto obj = yyjson_mut_obj(doc);
		for (idx_t k = 0; k < key_indexes.size(); k++) {
			TierDBAddCell(doc, obj, key_names[k].c_str(), chunk.GetValue(key_indexes[k], row));
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

SinkResultType PhysicalTierDBDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	if (chunk.size() == 0) {
		return SinkResultType::NEED_MORE_INPUT;
	}
	chunk.Flatten();

	auto rows_json = KeysToJsonArray(chunk, key_indexes, key_names);
	auto &client = table.catalog.Cast<TierDBCatalog>().GetClient();
	TierDBTransaction::Get(context.client, table.catalog).EnsureWriteTxn(client);
	auto result = client.DeleteChunk(table.tierdb_schema.schema_name, table.tierdb_schema.table_name, rows_json);
	TierDBExecuteLakeDml(context.client, result);

	auto &gstate = input.global_state.Cast<TierDBDeleteGlobalState>();
	lock_guard<mutex> guard(gstate.lock);
	gstate.delete_count += NumericCast<idx_t>(result.count);
	return SinkResultType::NEED_MORE_INPUT;
}

SourceResultType PhysicalTierDBDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<TierDBDeleteGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.delete_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
