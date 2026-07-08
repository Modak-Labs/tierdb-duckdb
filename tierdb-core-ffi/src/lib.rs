// Every entry point is a null-checked C ABI shim; the caller contract lives in
// the C++ header (include/tierdb/ffi.hpp), not in per-function Rust docs.
#![allow(clippy::missing_safety_doc)]

mod abi;
mod catalog;
mod scan;
mod write;

use std::ffi::{c_char, CStr, CString};

use abi::{TierDBConnResult, TierDBStatus, TierDBStringResult};
use catalog::{Conn, TableSchema};

fn borrow_str<'a>(ptr: *const c_char, what: &str) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err(format!("{what} pointer was null"));
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|_| format!("{what} was not valid UTF-8"))
}

fn table_schema_json(s: &TableSchema) -> String {
    let columns: Vec<_> = s
        .columns
        .iter()
        .map(|(name, pg_type)| {
            serde_json::json!({
                "name": name,
                "pg_type": pg_type,
                "duckdb_type": tierdb_core::dialect::duckdb_type(pg_type),
            })
        })
        .collect();
    serde_json::json!({
        "table_id": s.table_id,
        "schema_name": s.schema_name,
        "table_name": s.table_name,
        "lake_format": s.lake_format,
        "lake_table_ref": s.lake_table_ref,
        "tier_key_col": s.tier_key_col,
        "tier_key_type": s.tier_key_type.name(),
        "primary_key_cols": s.primary_key_cols,
        "columns": columns,
        "mode": s.mode,
        "heap_retention_lag": s.heap_retention_lag,
        "lake_retention_lag": s.lake_retention_lag,
    })
    .to_string()
}

#[no_mangle]
pub unsafe extern "C" fn tierdb_connect(dsn: *const c_char) -> TierDBConnResult {
    let dsn = match borrow_str(dsn, "dsn") {
        Ok(s) => s,
        Err(e) => return TierDBConnResult::err(e),
    };
    match Conn::connect(dsn) {
        Ok(conn) => TierDBConnResult::ok(conn),
        Err(e) => TierDBConnResult::err(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tierdb_conn_free(conn: *mut Conn) {
    if !conn.is_null() {
        drop(Box::from_raw(conn));
    }
}

#[no_mangle]
pub unsafe extern "C" fn tierdb_table_schema(
    conn: *mut Conn,
    schema: *const c_char,
    table: *const c_char,
) -> TierDBStringResult {
    let Some(conn) = conn.as_mut() else {
        return TierDBStringResult::err("connection pointer was null");
    };
    let schema = match borrow_str(schema, "schema") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    let table = match borrow_str(table, "table") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    match conn.table_schema(schema, table) {
        Ok(s) => TierDBStringResult::ok(table_schema_json(&s)),
        Err(e) => TierDBStringResult::err(e),
    }
}

/// Pins the table's cut-line and returns `{ "pin_id": <i64>|null, "scan_sql": ".." }`.
/// The caller must hold `pin_id` for the transaction and release it with
/// `tierdb_release_read_pin`; a null `pin_id` means a hot-only read that took no pin.
#[no_mangle]
pub unsafe extern "C" fn tierdb_acquire_read_scan(
    conn: *mut Conn,
    schema: *const c_char,
    table: *const c_char,
) -> TierDBStringResult {
    let Some(conn) = conn.as_mut() else {
        return TierDBStringResult::err("connection pointer was null");
    };
    let schema = match borrow_str(schema, "schema") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    let table = match borrow_str(table, "table") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    match scan::acquire_scan(conn, schema, table) {
        Ok(pinned) => TierDBStringResult::ok(
            serde_json::json!({
                "pin_id": pinned.pin_id,
                "scan_sql": pinned.scan_sql,
            })
            .to_string(),
        ),
        Err(e) => TierDBStringResult::err(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tierdb_release_read_pin(conn: *mut Conn, pin_id: i64) -> TierDBStatus {
    let Some(conn) = conn.as_mut() else {
        return TierDBStatus::err("connection pointer was null");
    };
    match scan::release_pin(conn, pin_id) {
        Ok(()) => TierDBStatus::ok(),
        Err(e) => TierDBStatus::err(e),
    }
}

/// Opens the write transaction that spans every chunk and statement of the
/// current DuckDB transaction. Idempotent, so the first write begins it.
#[no_mangle]
pub unsafe extern "C" fn tierdb_txn_begin(conn: *mut Conn) -> TierDBStatus {
    let Some(conn) = conn.as_mut() else {
        return TierDBStatus::err("connection pointer was null");
    };
    match conn.txn_begin() {
        Ok(()) => TierDBStatus::ok(),
        Err(e) => TierDBStatus::err(e),
    }
}

/// Commits the write transaction at the DuckDB commit boundary. A no-op when no
/// write transaction is open (a read-only DuckDB transaction).
#[no_mangle]
pub unsafe extern "C" fn tierdb_txn_commit(conn: *mut Conn) -> TierDBStatus {
    let Some(conn) = conn.as_mut() else {
        return TierDBStatus::err("connection pointer was null");
    };
    match conn.txn_commit() {
        Ok(()) => TierDBStatus::ok(),
        Err(e) => TierDBStatus::err(e),
    }
}

/// Rolls the write transaction back at the DuckDB rollback boundary. A no-op when
/// no write transaction is open.
#[no_mangle]
pub unsafe extern "C" fn tierdb_txn_rollback(conn: *mut Conn) -> TierDBStatus {
    let Some(conn) = conn.as_mut() else {
        return TierDBStatus::err("connection pointer was null");
    };
    match conn.txn_rollback() {
        Ok(()) => TierDBStatus::ok(),
        Err(e) => TierDBStatus::err(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tierdb_insert_chunk(
    conn: *mut Conn,
    schema: *const c_char,
    table: *const c_char,
    rows_json: *const c_char,
) -> TierDBStringResult {
    let Some(conn) = conn.as_mut() else {
        return TierDBStringResult::err("connection pointer was null");
    };
    let schema = match borrow_str(schema, "schema") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    let table = match borrow_str(table, "table") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    let rows_json = match borrow_str(rows_json, "rows") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    match write::insert_chunk(conn, schema, table, rows_json) {
        Ok(n) => TierDBStringResult::ok(n.to_string()),
        Err(e) => TierDBStringResult::err(e),
    }
}

/// Deletes the rows described by `rows_json` (a JSON array whose objects carry
/// the primary-key columns and the tier-key column). Each row is removed from
/// the heap, the lake overlay, or both per the table's shape and cut-line.
#[no_mangle]
pub unsafe extern "C" fn tierdb_delete_chunk(
    conn: *mut Conn,
    schema: *const c_char,
    table: *const c_char,
    rows_json: *const c_char,
) -> TierDBStringResult {
    let Some(conn) = conn.as_mut() else {
        return TierDBStringResult::err("connection pointer was null");
    };
    let schema = match borrow_str(schema, "schema") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    let table = match borrow_str(table, "table") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    let rows_json = match borrow_str(rows_json, "rows") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    match write::delete_chunk(conn, schema, table, rows_json) {
        Ok(n) => TierDBStringResult::ok(n.to_string()),
        Err(e) => TierDBStringResult::err(e),
    }
}

/// Updates the rows described by `rows_json` (a JSON array of `{ "old": {routing
/// key}, "new": {full row image} }` objects). Each update removes the row's old
/// placement and writes its new one, carrying cross-tier moves and key changes.
#[no_mangle]
pub unsafe extern "C" fn tierdb_update_chunk(
    conn: *mut Conn,
    schema: *const c_char,
    table: *const c_char,
    rows_json: *const c_char,
) -> TierDBStringResult {
    let Some(conn) = conn.as_mut() else {
        return TierDBStringResult::err("connection pointer was null");
    };
    let schema = match borrow_str(schema, "schema") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    let table = match borrow_str(table, "table") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    let rows_json = match borrow_str(rows_json, "rows") {
        Ok(s) => s,
        Err(e) => return TierDBStringResult::err(e),
    };
    match write::update_chunk(conn, schema, table, rows_json) {
        Ok(n) => TierDBStringResult::ok(n.to_string()),
        Err(e) => TierDBStringResult::err(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tierdb_list_tables(conn: *mut Conn) -> TierDBStringResult {
    let Some(conn) = conn.as_mut() else {
        return TierDBStringResult::err("connection pointer was null");
    };
    match conn.list_tables() {
        Ok(tables) => {
            let arr: Vec<_> = tables
                .into_iter()
                .map(|(schema, table)| {
                    serde_json::json!({ "schema_name": schema, "table_name": table })
                })
                .collect();
            TierDBStringResult::ok(serde_json::Value::Array(arr).to_string())
        }
        Err(e) => TierDBStringResult::err(e),
    }
}

#[no_mangle]
pub unsafe extern "C" fn tierdb_free_string(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}
