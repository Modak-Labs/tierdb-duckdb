#include "storage/tierdb_json.hpp"

#include "duckdb/common/types/value.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

yyjson_mut_val *TierDBValueToJson(yyjson_mut_doc *doc, const Value &value) {
	if (value.IsNull()) {
		return yyjson_mut_null(doc);
	}
	switch (value.type().id()) {
	case LogicalTypeId::BOOLEAN:
		return yyjson_mut_bool(doc, value.GetValue<bool>());
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return yyjson_mut_sint(doc, value.GetValue<int64_t>());
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return yyjson_mut_uint(doc, value.GetValue<uint64_t>());
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return yyjson_mut_real(doc, value.GetValue<double>());
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		auto arr = yyjson_mut_arr(doc);
		auto &children = value.type().id() == LogicalTypeId::ARRAY ? ArrayValue::GetChildren(value)
		                                                           : ListValue::GetChildren(value);
		for (auto &child : children) {
			yyjson_mut_arr_append(arr, TierDBValueToJson(doc, child));
		}
		return arr;
	}
	case LogicalTypeId::STRUCT: {
		auto obj = yyjson_mut_obj(doc);
		auto &children = StructValue::GetChildren(value);
		for (idx_t i = 0; i < children.size(); i++) {
			auto &name = StructType::GetChildName(value.type(), i);
			auto jkey = yyjson_mut_strcpy(doc, name.c_str());
			yyjson_mut_obj_add(obj, jkey, TierDBValueToJson(doc, children[i]));
		}
		return obj;
	}
	default: {
		auto text = value.ToString();
		return yyjson_mut_strcpy(doc, text.c_str());
	}
	}
}

void TierDBAddCell(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key, const Value &value) {
	auto jkey = yyjson_mut_strcpy(doc, key);
	yyjson_mut_obj_add(obj, jkey, TierDBValueToJson(doc, value));
}

} // namespace duckdb
