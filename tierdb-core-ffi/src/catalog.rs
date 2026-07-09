use std::collections::BTreeMap;

use postgres::{Client, NoTls};
use tierdb_core::domain::TableId;
use tierdb_core::lake::LakeCatalog;
use tierdb_core::mode::Mode;
use tierdb_core::sqlgen::Column;
use tierdb_core::table::Table;
use tierdb_core::TierKeyType;

pub struct Conn {
    pub client: Client,
    pub dsn: String,
    /// True while a caller-managed write transaction is open on this connection,
    /// so writes span statements and the read-pin acquire joins it instead of
    /// opening a nested `BEGIN`.
    pub in_txn: bool,
}

impl Conn {
    pub fn connect(dsn: &str) -> Result<Conn, String> {
        let client = Client::connect(dsn, NoTls).map_err(|e| format!("connect failed: {e}"))?;
        Ok(Conn {
            client,
            dsn: dsn.to_string(),
            in_txn: false,
        })
    }

    /// Opens the write transaction that the DuckDB transaction boundary later
    /// commits or rolls back. Idempotent within one transaction, so the first
    /// write begins it and the rest run inside it.
    pub fn txn_begin(&mut self) -> Result<(), String> {
        if self.in_txn {
            return Ok(());
        }
        self.client
            .batch_execute("BEGIN")
            .map_err(|e| format!("BEGIN failed: {e}"))?;
        self.in_txn = true;
        Ok(())
    }

    pub fn txn_commit(&mut self) -> Result<(), String> {
        if !self.in_txn {
            return Ok(());
        }
        self.client
            .batch_execute("COMMIT")
            .map_err(|e| format!("COMMIT failed: {e}"))?;
        self.in_txn = false;
        Ok(())
    }

    pub fn txn_rollback(&mut self) -> Result<(), String> {
        if !self.in_txn {
            return Ok(());
        }
        self.client
            .batch_execute("ROLLBACK")
            .map_err(|e| format!("ROLLBACK failed: {e}"))?;
        self.in_txn = false;
        Ok(())
    }
}

pub struct Cutline {
    pub tier_key_hi: i64,
    pub lake_snapshot_id: i64,
    pub lake_props: BTreeMap<String, String>,
}

fn parse_props(text: &str, what: &str) -> Result<BTreeMap<String, String>, String> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| format!("{what} is not valid JSON: {e}"))?;
    let mut out = BTreeMap::new();
    if let serde_json::Value::Object(map) = value {
        for (k, v) in map {
            let s = match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            out.insert(k, s);
        }
    }
    Ok(out)
}

pub struct TableSchema {
    pub table_id: i64,
    pub schema_name: String,
    pub table_name: String,
    pub lake_format: String,
    pub lake_table_ref: String,
    pub tier_key_col: String,
    pub tier_key_type: TierKeyType,
    pub primary_key_cols: Vec<String>,
    pub columns: Vec<(String, String)>,
    pub mode: String,
    pub keep_heap: bool,
    pub heap_retention_lag: Option<i64>,
    pub lake_retention_lag: Option<i64>,
    pub storage_profile: String,
}

impl TableSchema {
    pub fn mode(&self) -> Result<Mode, String> {
        Mode::from_catalog(&self.mode, self.keep_heap, self.heap_retention_lag)
            .map_err(|e| e.to_string())
    }

    /// Builds a [`Table`] from this schema. `catalog` is folded in by callers
    /// that resolve a live catalog attachment (direct mode); it stays `None`
    /// for tiered/mirrored reads that never touch the live catalog.
    pub fn table(&self, catalog: Option<LakeCatalog>) -> Result<Table, String> {
        Ok(Table {
            id: TableId(self.table_id as u32),
            schema: self.schema_name.clone(),
            name: self.table_name.clone(),
            columns: self
                .columns
                .iter()
                .map(|(name, sql_type)| Column {
                    name: name.clone(),
                    sql_type: sql_type.clone(),
                })
                .collect(),
            pk_cols: self.primary_key_cols.clone(),
            tier_key_col: self.tier_key_col.clone(),
            tier_key_type: self.tier_key_type,
            mode: self.mode()?,
            lake_format: self.lake_format.clone(),
            lake_table_ref: Some(self.lake_table_ref.clone()),
            catalog,
        })
    }
}

impl Conn {
    pub fn table_schema(&mut self, schema: &str, table: &str) -> Result<TableSchema, String> {
        let row = self
            .client
            .query_opt(
                "SELECT table_id, schema_name, table_name, lake_format, lake_table_ref, \
                        tier_key_col, tier_key_type, primary_key_cols, \
                        mode, keep_heap, heap_retention_lag, lake_retention_lag, \
                        storage_profile \
                   FROM tierdb.tables \
                  WHERE schema_name = $1 AND table_name = $2",
                &[&schema, &table],
            )
            .map_err(|e| format!("tierdb.tables lookup failed: {e}"))?
            .ok_or_else(|| {
                format!("table \"{schema}\".\"{table}\" is not registered with tierdb")
            })?;

        let table_id: i64 = row.get("table_id");
        let tier_key_type_name: String = row.get("tier_key_type");
        let tier_key_type = TierKeyType::from_name(&tier_key_type_name)
            .map_err(|e| format!("table \"{schema}\".\"{table}\": {e}"))?;

        let columns = self.heap_columns(schema, table)?;

        Ok(TableSchema {
            table_id,
            schema_name: row.get("schema_name"),
            table_name: row.get("table_name"),
            lake_format: row.get("lake_format"),
            lake_table_ref: row.get("lake_table_ref"),
            tier_key_col: row.get("tier_key_col"),
            tier_key_type,
            primary_key_cols: row.get("primary_key_cols"),
            columns,
            mode: row.get("mode"),
            keep_heap: row.get("keep_heap"),
            heap_retention_lag: row.get("heap_retention_lag"),
            lake_retention_lag: row.get("lake_retention_lag"),
            storage_profile: row.get("storage_profile"),
        })
    }

    /// The table's live catalog attachment, resolved from its storage profile;
    /// `Ok(None)` when none is configured.
    pub fn lake_catalog(&mut self, ts: &TableSchema) -> Result<Option<LakeCatalog>, String> {
        let row = self
            .client
            .query_opt(
                "SELECT warehouse, lake_config::text AS lake_config \
                   FROM tierdb.storage_profiles WHERE profile_name = $1",
                &[&ts.storage_profile],
            )
            .map_err(|e| format!("tierdb.storage_profiles lookup failed: {e}"))?
            .ok_or_else(|| format!("storage profile '{}' is not registered", ts.storage_profile))?;
        let warehouse: String = row.get("warehouse");
        let config = match row.get::<_, Option<String>>("lake_config") {
            Some(text) => parse_props(&text, "tierdb.storage_profiles.lake_config")?,
            None => BTreeMap::new(),
        };
        LakeCatalog::from_profile(&ts.lake_format, &ts.storage_profile, &warehouse, &config)
            .map_err(|e| e.to_string())
    }

    fn heap_columns(&mut self, schema: &str, table: &str) -> Result<Vec<(String, String)>, String> {
        let qualified = format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            table.replace('"', "\"\"")
        );
        let rows = self
            .client
            .query(
                "SELECT a.attname AS name, \
                        format_type(a.atttypid, a.atttypmod) AS pg_type \
                   FROM pg_attribute a \
                  WHERE a.attrelid = to_regclass($1)::oid \
                    AND a.attnum > 0 AND NOT a.attisdropped \
                  ORDER BY a.attnum",
                &[&qualified],
            )
            .map_err(|e| format!("column lookup for {qualified} failed: {e}"))?;
        if rows.is_empty() {
            return Err(format!(
                "heap relation {qualified} has no columns or does not exist"
            ));
        }
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<_, String>("name"), r.get::<_, String>("pg_type")))
            .collect())
    }

    pub fn cutline(&mut self, table_id: i64) -> Result<Option<Cutline>, String> {
        let row = self
            .client
            .query_opt(
                "SELECT tier_key_hi, lake_snapshot_id, lake_props::text AS lake_props \
                   FROM tierdb.cutline WHERE table_id = $1",
                &[&table_id],
            )
            .map_err(|e| format!("tierdb.cutline lookup failed: {e}"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let props_text: Option<String> = row.get("lake_props");
        let lake_props = match props_text {
            Some(text) => parse_props(&text, "tierdb.cutline.lake_props")?,
            None => BTreeMap::new(),
        };
        Ok(Some(Cutline {
            tier_key_hi: row.get("tier_key_hi"),
            lake_snapshot_id: row.get("lake_snapshot_id"),
            lake_props,
        }))
    }

    pub fn retention_line(&mut self, table_id: i64) -> Result<Option<i64>, String> {
        let row = self
            .client
            .query_opt(
                "SELECT retention_line FROM tierdb.cutline WHERE table_id = $1",
                &[&table_id],
            )
            .map_err(|e| format!("tierdb.cutline retention lookup failed: {e}"))?;
        Ok(row.and_then(|r| r.get::<_, Option<i64>>("retention_line")))
    }

    pub fn list_tables(&mut self) -> Result<Vec<(String, String)>, String> {
        let rows = self
            .client
            .query(
                "SELECT schema_name, table_name FROM tierdb.tables \
                  ORDER BY schema_name, table_name",
                &[],
            )
            .map_err(|e| format!("tierdb.tables scan failed: {e}"))?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<_, String>("schema_name"),
                    r.get::<_, String>("table_name"),
                )
            })
            .collect())
    }
}
