#include "tierdb/client.hpp"
#include "tierdb/ffi.hpp"

#include "duckdb.hpp"
#include "yyjson.hpp"

#include <cstdlib>

using namespace duckdb_yyjson;

namespace duckdb {

TierDBClient::TierDBClient(const string &dsn) {
	auto res = tierdb_connect(dsn.c_str());
	if (res.err) {
		string message = res.err;
		tierdb_free_string(res.err);
		throw IOException("tierdb: %s", message);
	}
	conn = res.value;
}

TierDBClient::~TierDBClient() {
	if (conn) {
		tierdb_conn_free(conn);
		conn = nullptr;
	}
}

static string ObjStr(yyjson_val *obj, const char *key) {
	auto val = yyjson_obj_get(obj, key);
	auto str = val ? yyjson_get_str(val) : nullptr;
	return str ? string(str) : string();
}

vector<TierDBTableRef> TierDBClient::ListTables() {
	TierDBStringResult res;
	{
		lock_guard<mutex> guard(conn_lock);
		res = tierdb_list_tables(conn);
	}
	if (res.err) {
		string message = res.err;
		tierdb_free_string(res.err);
		throw IOException("tierdb: %s", message);
	}

	vector<TierDBTableRef> tables;
	auto doc = yyjson_read(res.value, strlen(res.value), 0);
	if (doc) {
		auto root = yyjson_doc_get_root(doc);
		size_t idx, max;
		yyjson_val *entry;
		yyjson_arr_foreach(root, idx, max, entry) {
			TierDBTableRef ref;
			ref.schema_name = ObjStr(entry, "schema_name");
			ref.table_name = ObjStr(entry, "table_name");
			tables.push_back(std::move(ref));
		}
		yyjson_doc_free(doc);
	}
	tierdb_free_string(res.value);
	return tables;
}

bool TierDBClient::TryGetTableSchema(const string &schema, const string &table, TierDBTableSchema &result) {
	TierDBStringResult res;
	{
		lock_guard<mutex> guard(conn_lock);
		res = tierdb_table_schema(conn, schema.c_str(), table.c_str());
	}
	if (res.err) {
		string message = res.err;
		tierdb_free_string(res.err);
		if (StringUtil::Contains(message, "not registered")) {
			return false;
		}
		throw IOException("tierdb: %s", message);
	}

	auto doc = yyjson_read(res.value, strlen(res.value), 0);
	if (!doc) {
		tierdb_free_string(res.value);
		throw IOException("tierdb: could not parse table schema for \"%s\".\"%s\"", schema, table);
	}
	auto root = yyjson_doc_get_root(doc);

	auto table_id_val = yyjson_obj_get(root, "table_id");
	result.table_id = table_id_val ? yyjson_get_sint(table_id_val) : 0;
	result.schema_name = ObjStr(root, "schema_name");
	result.table_name = ObjStr(root, "table_name");
	result.lake_format = ObjStr(root, "lake_format");
	result.lake_table_ref = ObjStr(root, "lake_table_ref");
	result.tier_key_col = ObjStr(root, "tier_key_col");
	result.tier_key_type = ObjStr(root, "tier_key_type");

	result.primary_key_cols.clear();
	auto pk = yyjson_obj_get(root, "primary_key_cols");
	if (pk) {
		size_t idx, max;
		yyjson_val *v;
		yyjson_arr_foreach(pk, idx, max, v) {
			auto str = yyjson_get_str(v);
			if (str) {
				result.primary_key_cols.emplace_back(str);
			}
		}
	}

	result.columns.clear();
	auto cols = yyjson_obj_get(root, "columns");
	if (cols) {
		size_t idx, max;
		yyjson_val *v;
		yyjson_arr_foreach(cols, idx, max, v) {
			TierDBColumn col;
			col.name = ObjStr(v, "name");
			col.pg_type = ObjStr(v, "pg_type");
			col.duckdb_type = ObjStr(v, "duckdb_type");
			result.columns.push_back(std::move(col));
		}
	}

	yyjson_doc_free(doc);
	tierdb_free_string(res.value);
	return true;
}

TierDBPinnedScan TierDBClient::AcquireReadScan(const string &schema, const string &table) {
	TierDBStringResult res;
	{
		lock_guard<mutex> guard(conn_lock);
		res = tierdb_acquire_read_scan(conn, schema.c_str(), table.c_str());
	}
	if (res.err) {
		string message = res.err;
		tierdb_free_string(res.err);
		throw IOException("tierdb: %s", message);
	}

	auto doc = yyjson_read(res.value, strlen(res.value), 0);
	if (!doc) {
		tierdb_free_string(res.value);
		throw IOException("tierdb: could not parse pinned scan for \"%s\".\"%s\"", schema, table);
	}
	auto root = yyjson_doc_get_root(doc);
	auto pin_val = yyjson_obj_get(root, "pin_id");

	TierDBPinnedScan pinned;
	pinned.has_pin = pin_val && !yyjson_is_null(pin_val);
	pinned.pin_id = pinned.has_pin ? yyjson_get_sint(pin_val) : 0;
	pinned.scan_sql = ObjStr(root, "scan_sql");
	pinned.attach_sql = ObjStr(root, "attach_sql");

	yyjson_doc_free(doc);
	tierdb_free_string(res.value);
	return pinned;
}

void TierDBClient::ReleaseReadPin(int64_t pin_id) {
	TierDBStatus status;
	{
		lock_guard<mutex> guard(conn_lock);
		status = tierdb_release_read_pin(conn, pin_id);
	}
	if (status.err) {
		string message = status.err;
		tierdb_free_string(status.err);
		throw IOException("tierdb: %s", message);
	}
}

void TierDBClient::TxnBegin() {
	TierDBStatus status;
	{
		lock_guard<mutex> guard(conn_lock);
		status = tierdb_txn_begin(conn);
	}
	if (status.err) {
		string message = status.err;
		tierdb_free_string(status.err);
		throw IOException("tierdb: %s", message);
	}
}

void TierDBClient::TxnCommit() {
	TierDBStatus status;
	{
		lock_guard<mutex> guard(conn_lock);
		status = tierdb_txn_commit(conn);
	}
	if (status.err) {
		string message = status.err;
		tierdb_free_string(status.err);
		throw IOException("tierdb: %s", message);
	}
}

void TierDBClient::TxnRollback() {
	TierDBStatus status;
	{
		lock_guard<mutex> guard(conn_lock);
		status = tierdb_txn_rollback(conn);
	}
	if (status.err) {
		string message = status.err;
		tierdb_free_string(status.err);
		throw IOException("tierdb: %s", message);
	}
}

static TierDBDmlResult ParseDmlResult(TierDBStringResult res) {
	if (res.err) {
		string message = res.err;
		tierdb_free_string(res.err);
		throw IOException("tierdb: %s", message);
	}
	TierDBDmlResult result;
	result.count = 0;
	auto doc = yyjson_read(res.value, strlen(res.value), 0);
	if (!doc) {
		tierdb_free_string(res.value);
		throw IOException("tierdb: could not parse DML result");
	}
	auto root = yyjson_doc_get_root(doc);
	auto count_val = yyjson_obj_get(root, "count");
	result.count = count_val ? yyjson_get_sint(count_val) : 0;
	result.attach_sql = ObjStr(root, "attach_sql");
	auto lake = yyjson_obj_get(root, "lake_sql");
	if (lake) {
		size_t idx, max;
		yyjson_val *v;
		yyjson_arr_foreach(lake, idx, max, v) {
			auto str = yyjson_get_str(v);
			if (str) {
				result.lake_sql.emplace_back(str);
			}
		}
	}
	yyjson_doc_free(doc);
	tierdb_free_string(res.value);
	return result;
}

TierDBDmlResult TierDBClient::InsertChunk(const string &schema, const string &table, const string &rows_json) {
	TierDBStringResult res;
	{
		lock_guard<mutex> guard(conn_lock);
		res = tierdb_insert_chunk(conn, schema.c_str(), table.c_str(), rows_json.c_str());
	}
	return ParseDmlResult(res);
}

TierDBDmlResult TierDBClient::DeleteChunk(const string &schema, const string &table, const string &rows_json) {
	TierDBStringResult res;
	{
		lock_guard<mutex> guard(conn_lock);
		res = tierdb_delete_chunk(conn, schema.c_str(), table.c_str(), rows_json.c_str());
	}
	return ParseDmlResult(res);
}

TierDBDmlResult TierDBClient::UpdateChunk(const string &schema, const string &table, const string &rows_json) {
	TierDBStringResult res;
	{
		lock_guard<mutex> guard(conn_lock);
		res = tierdb_update_chunk(conn, schema.c_str(), table.c_str(), rows_json.c_str());
	}
	return ParseDmlResult(res);
}

static void RunOnFreshConnection(ClientContext &context, const string &sql, const char *what) {
	Connection con(*context.db);
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw IOException("tierdb: %s failed: %s", what, result->GetError());
	}
}

void TierDBExecuteLakeDml(ClientContext &context, const TierDBDmlResult &dml) {
	if (dml.lake_sql.empty()) {
		return;
	}
	if (!dml.attach_sql.empty()) {
		RunOnFreshConnection(context, dml.attach_sql, "lake catalog attach");
	}
	Connection con(*context.db);
	for (auto &sql : dml.lake_sql) {
		// Maintenance commits outside the advisory lock can still conflict;
		// the statement failed whole, so a bounded retry is safe.
		constexpr int max_attempts = 3;
		for (int attempt = 1;; attempt++) {
			auto result = con.Query(sql);
			if (!result->HasError()) {
				break;
			}
			auto error = result->GetError();
			bool conflict = StringUtil::Contains(error, "409") || StringUtil::Contains(error, "Conflict") ||
			                StringUtil::Contains(error, "CommitFailed");
			if (!conflict || attempt >= max_attempts) {
				throw IOException("tierdb: lake write failed: %s", error);
			}
		}
	}
}

LogicalType TierDBLogicalTypeFromString(const string &duckdb_type) {
	const string &t = duckdb_type;
	if (StringUtil::EndsWith(t, "[]")) {
		return LogicalType::LIST(TierDBLogicalTypeFromString(t.substr(0, t.size() - 2)));
	}
	string upper = StringUtil::Upper(t);
	if (StringUtil::StartsWith(upper, "DECIMAL(")) {
		auto open = t.find('(');
		auto close = t.find(')');
		auto args = t.substr(open + 1, close - open - 1);
		auto comma = args.find(',');
		auto width = static_cast<uint8_t>(std::stoi(args.substr(0, comma)));
		auto scale = static_cast<uint8_t>(std::stoi(args.substr(comma + 1)));
		return LogicalType::DECIMAL(width, scale);
	}
	if (upper == "BIGINT") {
		return LogicalType::BIGINT;
	}
	if (upper == "INTEGER") {
		return LogicalType::INTEGER;
	}
	if (upper == "SMALLINT") {
		return LogicalType::SMALLINT;
	}
	if (upper == "BOOLEAN") {
		return LogicalType::BOOLEAN;
	}
	if (upper == "DOUBLE") {
		return LogicalType::DOUBLE;
	}
	if (upper == "REAL") {
		return LogicalType::FLOAT;
	}
	if (upper == "DATE") {
		return LogicalType::DATE;
	}
	if (upper == "TIME") {
		return LogicalType::TIME;
	}
	if (upper == "TIME WITH TIME ZONE") {
		return LogicalType::TIME_TZ;
	}
	if (upper == "TIMESTAMP") {
		return LogicalType::TIMESTAMP;
	}
	if (upper == "TIMESTAMP WITH TIME ZONE") {
		return LogicalType::TIMESTAMP_TZ;
	}
	if (upper == "UUID") {
		return LogicalType::UUID;
	}
	if (upper == "BLOB") {
		return LogicalType::BLOB;
	}
	return LogicalType::VARCHAR;
}

} // namespace duckdb
