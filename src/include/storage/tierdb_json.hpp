#pragma once

#include "duckdb/common/types/value.hpp"
#include "yyjson.hpp"

namespace duckdb {

// Converts a DuckDB Value to JSON, recursing into LIST/ARRAY and STRUCT so
// nested types reach Postgres as real JSON rather than a stringified literal.
duckdb_yyjson::yyjson_mut_val *TierDBValueToJson(duckdb_yyjson::yyjson_mut_doc *doc, const Value &value);

void TierDBAddCell(duckdb_yyjson::yyjson_mut_doc *doc, duckdb_yyjson::yyjson_mut_val *obj, const char *key,
                   const Value &value);

} // namespace duckdb
