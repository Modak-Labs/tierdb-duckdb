#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class TierDBTableEntry;

class PhysicalTierDBInsert : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalTierDBInsert(PhysicalPlan &physical_plan, vector<LogicalType> types, TierDBTableEntry &table,
	                     idx_t estimated_cardinality);

	TierDBTableEntry &table;
	vector<string> column_names;

public:
	string GetName() const override {
		return "TIERDB_INSERT";
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
