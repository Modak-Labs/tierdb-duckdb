use serde_json::Value;
use tierdb_core::dml::{
    delta_write_sql, heap_delete_by_pk_sql, heap_insert_from_jsonb_sql, retention_rejects,
    TableShape, DELTA_OP_TOMBSTONE, DELTA_OP_UPSERT,
};
use tierdb_core::domain::{Cutline, LakeSnapshotId, RouteTarget, TierKey};
use tierdb_core::planner::route;
use tierdb_core::sqlgen::encode_pk;

use crate::catalog::{Conn, TableSchema};

fn field_text(row: &Value, field: &str) -> Result<String, String> {
    match row.get(field) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(Value::Number(n)) => Ok(n.to_string()),
        Some(Value::Bool(b)) => Ok(b.to_string()),
        Some(Value::Null) | None => Err(format!("row is missing required field '{field}'")),
        Some(other) => Err(format!("field '{field}' has unsupported type: {other}")),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn is_json_type(pg_type: &str) -> bool {
    let head = pg_type.trim().to_ascii_lowercase();
    let head = head.split('(').next().unwrap_or(&head).trim();
    matches!(head, "json" | "jsonb")
}

/// Reparse json/jsonb columns (the connector hands them to us as JSON strings)
/// so Postgres stores the object, not a scalar string.
fn normalize_json_columns(row: &mut Value, columns: &[(String, String)]) {
    let Value::Object(map) = row else {
        return;
    };
    for (name, ty) in columns {
        if !is_json_type(ty) {
            continue;
        }
        if let Some(Value::String(s)) = map.get(name) {
            if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                map.insert(name.clone(), parsed);
            }
        }
    }
}

fn retention_error(ts: &TableSchema, tier_key: i64, line: Option<i64>) -> String {
    format!(
        "tier_key {} is below the retention line {}; \
         rows this old have been expired from the lake",
        ts.tier_key_type.pg_literal(tier_key),
        ts.tier_key_type.pg_literal(line.unwrap_or_default()),
    )
}

fn tombstone_payload(row: &Value, pk_cols: &[String]) -> Value {
    let mut payload = serde_json::Map::new();
    for col in pk_cols {
        payload.insert(col.clone(), row.get(col).cloned().unwrap_or(Value::Null));
    }
    Value::Object(payload)
}

pub fn insert_chunk(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    rows_json: &str,
) -> Result<i64, String> {
    let ts = conn.table_schema(schema, table)?;
    let shape = TableShape::from_catalog(&ts.mode, ts.keep_heap, ts.heap_retention_lag)
        .map_err(|e| e.to_string())?;
    let cutline = conn.cutline(ts.table_id)?.map(|c| Cutline {
        t: TierKey(c.tier_key_hi),
        snapshot: LakeSnapshotId(c.lake_snapshot_id),
    });
    let retention = conn.retention_line(ts.table_id)?;

    let rows: Vec<Value> = serde_json::from_str(rows_json)
        .map_err(|e| format!("insert payload is not a JSON array of rows: {e}"))?;

    // Runs inside the transaction the DuckDB transaction boundary owns; a failure
    // here propagates and that boundary rolls the whole statement back.
    write_rows(conn, &ts, shape, cutline.as_ref(), retention, &rows)
}

fn write_rows(
    conn: &mut Conn,
    ts: &TableSchema,
    shape: TableShape,
    cutline: Option<&Cutline>,
    retention: Option<i64>,
    rows: &[Value],
) -> Result<i64, String> {
    let heap_sql = heap_insert_from_jsonb_sql(&ts.schema_name, &ts.table_name);
    let delta_sql = delta_write_sql();

    for row in rows {
        let mut row = row.clone();
        normalize_json_columns(&mut row, &ts.columns);
        let row = &row;
        let tier_text = field_text(row, &ts.tier_key_col)?;
        let tier_key = ts
            .tier_key_type
            .encode_text(&tier_text)
            .map_err(|e| e.to_string())?;
        let target = cutline
            .map(|c| route(TierKey(tier_key), c))
            .unwrap_or(RouteTarget::Hot);
        let plan = shape.plan_insert(target);

        if plan.to_delta {
            if plan.check_retention && retention_rejects(tier_key, retention) {
                return Err(retention_error(ts, tier_key, retention));
            }
            let pk_vals: Vec<String> = ts
                .primary_key_cols
                .iter()
                .map(|c| field_text(row, c))
                .collect::<Result<_, _>>()?;
            let pk = encode_pk(&pk_vals);
            conn.client
                .execute(
                    &delta_sql,
                    &[&ts.table_id, &pk, &DELTA_OP_UPSERT, &tier_key, row],
                )
                .map_err(|e| format!("delta write failed: {e}"))?;
        }
        if plan.to_heap {
            conn.client
                .execute(&heap_sql, &[row])
                .map_err(|e| format!("heap insert failed: {e}"))?;
        }
    }
    Ok(rows.len() as i64)
}

pub fn delete_chunk(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    rows_json: &str,
) -> Result<i64, String> {
    let ts = conn.table_schema(schema, table)?;
    let shape = TableShape::from_catalog(&ts.mode, ts.keep_heap, ts.heap_retention_lag)
        .map_err(|e| e.to_string())?;
    let cutline = conn.cutline(ts.table_id)?.map(|c| Cutline {
        t: TierKey(c.tier_key_hi),
        snapshot: LakeSnapshotId(c.lake_snapshot_id),
    });
    let retention = conn.retention_line(ts.table_id)?;

    let rows: Vec<Value> = serde_json::from_str(rows_json)
        .map_err(|e| format!("delete payload is not a JSON array of rows: {e}"))?;

    delete_rows(conn, &ts, shape, cutline.as_ref(), retention, &rows)
}

pub fn update_chunk(
    conn: &mut Conn,
    schema: &str,
    table: &str,
    rows_json: &str,
) -> Result<i64, String> {
    let ts = conn.table_schema(schema, table)?;
    let shape = TableShape::from_catalog(&ts.mode, ts.keep_heap, ts.heap_retention_lag)
        .map_err(|e| e.to_string())?;
    let cutline = conn.cutline(ts.table_id)?.map(|c| Cutline {
        t: TierKey(c.tier_key_hi),
        snapshot: LakeSnapshotId(c.lake_snapshot_id),
    });
    let retention = conn.retention_line(ts.table_id)?;

    // Each entry pairs the row's old routing key with its full new image.
    let pairs: Vec<Value> = serde_json::from_str(rows_json)
        .map_err(|e| format!("update payload is not a JSON array of rows: {e}"))?;
    let mut old_rows = Vec::with_capacity(pairs.len());
    let mut new_rows = Vec::with_capacity(pairs.len());
    for pair in pairs {
        let old = pair
            .get("old")
            .cloned()
            .ok_or_else(|| "update row is missing its 'old' key".to_string())?;
        let new = pair
            .get("new")
            .cloned()
            .ok_or_else(|| "update row is missing its 'new' image".to_string())?;
        old_rows.push(old);
        new_rows.push(new);
    }

    // Remove every row's old placement, then write its new one. Doing all the
    // removals first keeps a same-key move from deleting the row it just wrote.
    // Both halves run inside the DuckDB-owned transaction, so a failure in either
    // rolls the whole update back.
    delete_rows(conn, &ts, shape, cutline.as_ref(), retention, &old_rows)
        .and_then(|_| write_rows(conn, &ts, shape, cutline.as_ref(), retention, &new_rows))
}

fn delete_rows(
    conn: &mut Conn,
    ts: &TableSchema,
    shape: TableShape,
    cutline: Option<&Cutline>,
    retention: Option<i64>,
    rows: &[Value],
) -> Result<i64, String> {
    let delta_sql = delta_write_sql();
    // The floor guards a hot delete against a row that has since gone cold; it
    // is only meaningful when a cut-line exists to bound against.
    let floor = cutline.map(|c| {
        format!(
            "{} >= {}",
            quote_ident(&ts.tier_key_col),
            ts.tier_key_type.pg_literal(c.t.0)
        )
    });

    for row in rows {
        let tier_text = field_text(row, &ts.tier_key_col)?;
        let tier_key = ts
            .tier_key_type
            .encode_text(&tier_text)
            .map_err(|e| e.to_string())?;
        let target = cutline
            .map(|c| route(TierKey(tier_key), c))
            .unwrap_or(RouteTarget::Hot);
        let plan = shape.plan_delete(target);

        let pk_vals: Vec<String> = ts
            .primary_key_cols
            .iter()
            .map(|c| field_text(row, c))
            .collect::<Result<_, _>>()?;

        if plan.tombstone {
            if plan.check_retention && retention_rejects(tier_key, retention) {
                return Err(retention_error(ts, tier_key, retention));
            }
            let pk = encode_pk(&pk_vals);
            let payload = tombstone_payload(row, &ts.primary_key_cols);
            conn.client
                .execute(
                    &delta_sql,
                    &[&ts.table_id, &pk, &DELTA_OP_TOMBSTONE, &tier_key, &payload],
                )
                .map_err(|e| format!("delta tombstone failed: {e}"))?;
        }
        if plan.from_heap {
            let heap_floor = if plan.heap_floor {
                floor.as_deref()
            } else {
                None
            };
            let heap_sql = heap_delete_by_pk_sql(
                &ts.schema_name,
                &ts.table_name,
                &ts.primary_key_cols,
                heap_floor,
            );
            let params: Vec<&(dyn postgres::types::ToSql + Sync)> = pk_vals
                .iter()
                .map(|v| v as &(dyn postgres::types::ToSql + Sync))
                .collect();
            conn.client
                .execute(&heap_sql, &params)
                .map_err(|e| format!("heap delete failed: {e}"))?;
        }
    }
    Ok(rows.len() as i64)
}
