#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class TierDBTableEntry;

class PhysicalTierDBDelete : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalTierDBDelete(PhysicalPlan &physical_plan, vector<LogicalType> types, TierDBTableEntry &table,
	                     vector<idx_t> key_indexes, vector<string> key_names, idx_t estimated_cardinality);

	TierDBTableEntry &table;
	// Child-chunk column positions of the routing key (primary key columns then
	// the tier key), paired with the names they carry into the delete payload.
	vector<idx_t> key_indexes;
	vector<string> key_names;

public:
	string GetName() const override {
		return "TIERDB_DELETE";
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	bool IsSink() const override {
		return true;
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
