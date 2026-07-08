#include "storage/tierdb_schema.hpp"
#include "storage/tierdb_catalog.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/column_definition.hpp"

namespace duckdb {

TierDBSchemaEntry::TierDBSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) : SchemaCatalogEntry(catalog, info) {
}

unique_ptr<TierDBTableEntry> TierDBSchemaEntry::MakeTableEntry(const TierDBTableSchema &tierdb_schema) {
	CreateTableInfo info(*this, tierdb_schema.table_name);
	for (auto &column : tierdb_schema.columns) {
		info.columns.AddColumn(ColumnDefinition(column.name, TierDBLogicalTypeFromString(column.duckdb_type)));
	}
	return make_uniq<TierDBTableEntry>(catalog, *this, info, tierdb_schema);
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                          const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	auto &table_name = lookup_info.GetEntryName();
	auto &tierdb_catalog = catalog.Cast<TierDBCatalog>();

	lock_guard<mutex> guard(table_lock);
	auto entry = tables.find(table_name);
	if (entry != tables.end()) {
		return entry->second.get();
	}
	TierDBTableSchema tierdb_schema;
	if (!tierdb_catalog.GetClient().TryGetTableSchema(name, table_name, tierdb_schema)) {
		return nullptr;
	}
	auto table_entry = MakeTableEntry(tierdb_schema);
	auto result = table_entry.get();
	tables[table_name] = std::move(table_entry);
	return result;
}

void TierDBSchemaEntry::Scan(ClientContext &context, CatalogType type,
                             const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto &tierdb_catalog = catalog.Cast<TierDBCatalog>();
	auto refs = tierdb_catalog.GetClient().ListTables();

	lock_guard<mutex> guard(table_lock);
	for (auto &ref : refs) {
		if (!StringUtil::CIEquals(ref.schema_name, name)) {
			continue;
		}
		if (tables.find(ref.table_name) == tables.end()) {
			TierDBTableSchema tierdb_schema;
			if (!tierdb_catalog.GetClient().TryGetTableSchema(name, ref.table_name, tierdb_schema)) {
				continue;
			}
			tables[ref.table_name] = MakeTableEntry(tierdb_schema);
		}
		callback(*tables[ref.table_name]);
	}
}

void TierDBSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	lock_guard<mutex> guard(table_lock);
	for (auto &entry : tables) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	throw NotImplementedException("tierdb: creating tables through DuckDB is not supported");
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                             CreateFunctionInfo &info) {
	throw NotImplementedException("tierdb: functions are not supported");
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                          TableCatalogEntry &table) {
	throw NotImplementedException("tierdb: indexes are not supported");
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("tierdb: views are not supported");
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                             CreateSequenceInfo &info) {
	throw NotImplementedException("tierdb: sequences are not supported");
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                  CreateTableFunctionInfo &info) {
	throw NotImplementedException("tierdb: table functions are not supported");
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                 CreateCopyFunctionInfo &info) {
	throw NotImplementedException("tierdb: copy functions are not supported");
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                   CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("tierdb: pragma functions are not supported");
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                              CreateCollationInfo &info) {
	throw NotImplementedException("tierdb: collations are not supported");
}

optional_ptr<CatalogEntry> TierDBSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("tierdb: types are not supported");
}

void TierDBSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("tierdb: ALTER is not supported");
}

void TierDBSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("tierdb: DROP is not supported");
}

} // namespace duckdb
