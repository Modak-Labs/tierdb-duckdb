#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class TierDBTableEntry;

class PhysicalTierDBUpdate : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalTierDBUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types, TierDBTableEntry &table,
	                     vector<idx_t> new_value_indexes, vector<string> new_col_names, vector<string> old_key_names,
	                     idx_t estimated_cardinality);

	TierDBTableEntry &table;
	// Child-chunk positions of the new row image, one per real column, paired
	// with the column names they carry.
	vector<idx_t> new_value_indexes;
	vector<string> new_col_names;
	// The old routing key columns (primary key then tier key) are the trailing
	// columns of the child chunk; these are the names they carry.
	vector<string> old_key_names;

public:
	string GetName() const override {
		return "TIERDB_UPDATE";
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
