use crate::catalog::{Conn, Cutline, TableSchema};
use tierdb_core::dialect::DuckDbNative;
use tierdb_core::domain::{TableId, TierKey};
use tierdb_core::sqlgen::{
    read_pin_acquire_sql, read_pin_release_sql, render_scan, Column, LakePin, ReadCut, TableMeta,
    READ_PIN_TTL_SECS,
};

fn to_meta(s: &TableSchema) -> TableMeta {
    TableMeta {
        table_id: TableId(s.table_id as u32),
        hot_schema: s.schema_name.clone(),
        hot_table: s.table_name.clone(),
        columns: s
            .columns
            .iter()
            .map(|(name, sql_type)| Column {
                name: name.clone(),
                sql_type: sql_type.clone(),
            })
            .collect(),
        pk_cols: s.primary_key_cols.clone(),
        tier_key_col: s.tier_key_col.clone(),
        tier_key_type: s.tier_key_type,
    }
}

pub fn render_scan_sql(
    schema: &TableSchema,
    cutline: Option<&Cutline>,
    dsn: &str,
) -> Result<String, String> {
    let meta = to_meta(schema);
    let pin = match cutline {
        Some(c) => LakePin::from_catalog_committed(
            &schema.lake_format,
            &c.lake_props,
            Some(c.lake_snapshot_id),
        )
        .map_err(|e| e.to_string())?,
        None => None,
    };
    let cut = cutline.map(|c| ReadCut {
        t: TierKey(c.tier_key_hi),
        pin: pin.as_ref(),
    });
    render_scan(&meta, cut, &DuckDbNative { dsn }).map_err(|e| e.to_string())
}

/// A read pin plus the merged read rendered against the pinned `(T, S)`. A hot
/// only table (no cut-line) needs no pin, so `pin_id` is `None`.
pub struct PinnedScan {
    pub pin_id: Option<i64>,
    pub scan_sql: String,
}

/// Freeze the table's cut-line, pin it, and render the merged read against that
/// pin under one repeatable-read snapshot, so the pin matches exactly what the
/// scan reads and the lake worker cannot reclaim it mid-read.
pub fn acquire_scan(conn: &mut Conn, schema: &str, table: &str) -> Result<PinnedScan, String> {
    let ts = conn.table_schema(schema, table)?;
    let dsn = conn.dsn.clone();

    // A write transaction may already be open on this connection (a read after a
    // write in the same DuckDB transaction). Join it so we do not nest a `BEGIN`;
    // the DuckDB boundary commits the pin along with the writes. Otherwise capture
    // the cut-line and pin atomically under a short repeatable-read transaction so
    // the pin matches exactly what the scan reads.
    if conn.in_txn {
        return pin_and_render(conn, &ts, &dsn);
    }
    conn.client
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .map_err(|e| format!("read pin BEGIN failed: {e}"))?;
    let pinned = pin_and_render(conn, &ts, &dsn);
    match &pinned {
        Ok(_) => conn
            .client
            .batch_execute("COMMIT")
            .map_err(|e| format!("read pin COMMIT failed: {e}"))?,
        Err(_) => {
            let _ = conn.client.batch_execute("ROLLBACK");
        }
    }
    pinned
}

fn pin_and_render(conn: &mut Conn, ts: &TableSchema, dsn: &str) -> Result<PinnedScan, String> {
    let cutline = conn.cutline(ts.table_id)?;
    let pin_id = match &cutline {
        Some(c) => {
            let row = conn
                .client
                .query_one(
                    read_pin_acquire_sql(),
                    &[
                        &ts.table_id,
                        &c.lake_snapshot_id,
                        &c.tier_key_hi,
                        &(READ_PIN_TTL_SECS as f64),
                    ],
                )
                .map_err(|e| format!("read pin acquire failed: {e}"))?;
            Some(row.get::<_, i64>("pin_id"))
        }
        None => None,
    };
    let scan_sql = render_scan_sql(ts, cutline.as_ref(), dsn)?;
    Ok(PinnedScan { pin_id, scan_sql })
}

pub fn release_pin(conn: &mut Conn, pin_id: i64) -> Result<(), String> {
    conn.client
        .execute(read_pin_release_sql(), &[&pin_id])
        .map_err(|e| format!("read pin release failed: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tierdb_core::TierKeyType;

    fn schema() -> TableSchema {
        TableSchema {
            table_id: 90001,
            schema_name: "public".into(),
            table_name: "events".into(),
            lake_format: "iceberg".into(),
            lake_table_ref: "warehouse.public.events".into(),
            tier_key_col: "event_time".into(),
            tier_key_type: TierKeyType::from_name("bigint").unwrap(),
            primary_key_cols: vec!["id".into()],
            columns: vec![
                ("id".into(), "bigint".into()),
                ("event_time".into(), "bigint".into()),
                ("val".into(), "text".into()),
            ],
            mode: "tiered".into(),
            keep_heap: false,
            heap_retention_lag: None,
            lake_retention_lag: None,
        }
    }

    fn cutline(metadata: Option<&str>) -> Cutline {
        let mut props = BTreeMap::new();
        if let Some(m) = metadata {
            props.insert("metadata_location".into(), m.into());
        }
        Cutline {
            tier_key_hi: 100,
            lake_snapshot_id: 7,
            lake_props: props,
        }
    }

    #[test]
    fn hot_only_without_cutline() {
        let sql = render_scan_sql(&schema(), None, "host=h").unwrap();
        assert!(sql.contains("postgres_scan('host=h', 'public', 'events')"));
        assert!(!sql.contains("UNION ALL"));
        assert!(!sql.contains("tierdb"));
    }

    #[test]
    fn cold_is_delta_only_without_lake_snapshot() {
        let sql = render_scan_sql(&schema(), Some(&cutline(None)), "host=h").unwrap();
        assert!(sql.contains("WHERE \"event_time\" >= 100"));
        assert!(sql.contains("WHERE \"event_time\" < 100"));
        assert!(!sql.contains("iceberg_scan"));
        assert!(!sql.contains("NOT EXISTS"));
        assert!(sql.contains("d.table_id = 90001 AND d.op = 0"));
    }

    #[test]
    fn unsupported_lake_format_errors_instead_of_degrading() {
        let mut s = schema();
        s.lake_format = "hudi".into();
        let err = render_scan_sql(&s, Some(&cutline(None)), "host=h").unwrap_err();
        assert!(err.contains("unsupported lake_format"), "{err}");
    }

    #[test]
    fn cold_merges_iceberg_base_with_delta() {
        let meta = "/wh/events/metadata/00002-abc.metadata.json";
        let sql = render_scan_sql(&schema(), Some(&cutline(Some(meta))), "host=h").unwrap();
        assert!(sql.contains(&format!("iceberg_scan('{meta}')")));
        assert!(sql.contains("NOT EXISTS"));
        assert!(sql.contains("d.pk = b.\"id\"::text"));
        assert!(sql.contains("d.table_id = 90001 AND d.op = 0"));
        assert!(sql.contains("WHERE \"event_time\" >= 100"));
        assert!(sql.contains("WHERE \"event_time\" < 100"));
    }
}
