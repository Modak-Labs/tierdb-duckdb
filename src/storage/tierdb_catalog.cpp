#include "storage/tierdb_catalog.hpp"
#include "storage/tierdb_schema.hpp"
#include "storage/tierdb_table.hpp"
#include "storage/tierdb_insert.hpp"
#include "storage/tierdb_delete.hpp"
#include "storage/tierdb_update.hpp"

#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

TierDBCatalog::TierDBCatalog(AttachedDatabase &db, string dsn_p, AccessMode access_mode_p)
    : Catalog(db), dsn(std::move(dsn_p)), access_mode(access_mode_p) {
	client = make_uniq<TierDBClient>(dsn);
}

TierDBCatalog::~TierDBCatalog() = default;

void TierDBCatalog::Initialize(bool load_builtin) {
}

void TierDBCatalog::LoadSchemas() {
	if (schemas_loaded) {
		return;
	}
	auto refs = client->ListTables();
	for (auto &ref : refs) {
		if (schemas.find(ref.schema_name) != schemas.end()) {
			continue;
		}
		CreateSchemaInfo info;
		info.catalog = GetName();
		info.schema = ref.schema_name;
		schemas[ref.schema_name] = make_uniq<TierDBSchemaEntry>(*this, info);
	}
	schemas_loaded = true;
}

optional_ptr<CatalogEntry> TierDBCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	throw NotImplementedException("tierdb: CREATE SCHEMA is not supported (schemas follow tierdb.tables)");
}

void TierDBCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	lock_guard<mutex> guard(schema_lock);
	LoadSchemas();
	for (auto &entry : schemas) {
		callback(*entry.second);
	}
}

optional_ptr<SchemaCatalogEntry> TierDBCatalog::LookupSchema(CatalogTransaction transaction,
                                                             const EntryLookupInfo &schema_lookup,
                                                             OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	lock_guard<mutex> guard(schema_lock);
	LoadSchemas();
	auto entry = schemas.find(schema_name);
	if (entry == schemas.end()) {
		if (if_not_found != OnEntryNotFound::RETURN_NULL) {
			throw BinderException("Schema \"%s\" not found in tierdb catalog", schema_name);
		}
		return nullptr;
	}
	return entry->second.get();
}

PhysicalOperator &TierDBCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("tierdb: CREATE TABLE AS is not implemented yet");
}

PhysicalOperator &TierDBCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                            optional_ptr<PhysicalOperator> plan) {
	if (!plan) {
		throw NotImplementedException("tierdb: INSERT without a source is not supported");
	}
	if (op.return_chunk) {
		throw NotImplementedException("tierdb: INSERT ... RETURNING is not supported yet");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw NotImplementedException("tierdb: INSERT ... ON CONFLICT is not supported (writes are newest-wins upserts)");
	}
	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}
	auto &table = op.table.Cast<TierDBTableEntry>();
	auto &insert = planner.Make<PhysicalTierDBInsert>(op.types, table, op.estimated_cardinality);
	insert.children.push_back(*plan);
	return insert;
}

PhysicalOperator &TierDBCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                            PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw NotImplementedException("tierdb: DELETE ... RETURNING is not supported yet");
	}
	auto &table = op.table.Cast<TierDBTableEntry>();

	// The binder projected the routing key (primary key columns then tier key)
	// as this table's row-id columns; each expression tells us where that column
	// lands in the child chunk, and RoutingColumns() names them in the same order.
	auto routing = table.RoutingColumns();
	if (op.expressions.size() != routing.size()) {
		throw InternalException("tierdb: delete row-id columns do not match the routing key");
	}
	vector<idx_t> key_indexes;
	vector<string> key_names;
	for (idx_t i = 0; i < op.expressions.size(); i++) {
		key_indexes.push_back(op.expressions[i]->Cast<BoundReferenceExpression>().index);
		key_names.push_back(routing[i].name);
	}

	auto &del = planner.Make<PhysicalTierDBDelete>(op.types, table, std::move(key_indexes), std::move(key_names),
	                                               op.estimated_cardinality);
	del.children.push_back(plan);
	return del;
}

PhysicalOperator &TierDBCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                            PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw NotImplementedException("tierdb: UPDATE ... RETURNING is not supported yet");
	}
	auto &table = op.table.Cast<TierDBTableEntry>();

	// BindUpdateConstraints forced del+insert over all columns, so the child
	// projects the full new row image (one column per table column) followed by
	// the old routing key columns. Map each new value to its child position.
	if (op.expressions.size() != op.columns.size()) {
		throw InternalException("tierdb: update expressions do not match the projected columns");
	}
	vector<idx_t> new_value_indexes;
	vector<string> new_col_names;
	for (idx_t i = 0; i < op.columns.size(); i++) {
		new_value_indexes.push_back(op.expressions[i]->Cast<BoundReferenceExpression>().index);
		new_col_names.push_back(table.GetColumns().GetColumn(op.columns[i]).Name());
	}

	vector<string> old_key_names;
	for (auto &rc : table.RoutingColumns()) {
		old_key_names.push_back(rc.name);
	}

	auto &update = planner.Make<PhysicalTierDBUpdate>(op.types, table, std::move(new_value_indexes),
	                                                  std::move(new_col_names), std::move(old_key_names),
	                                                  op.estimated_cardinality);
	update.children.push_back(plan);
	return update;
}

DatabaseSize TierDBCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	return size;
}

void TierDBCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("tierdb: DROP SCHEMA is not supported");
}

} // namespace duckdb
