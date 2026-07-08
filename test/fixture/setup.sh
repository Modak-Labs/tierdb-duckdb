#!/usr/bin/env bash
# Stand up the Postgres + Iceberg fixture the tierdb SQLLogicTests read from.
#
# Applies the tierdb catalog schema, generates the Iceberg lake snapshot, and
# registers table 90001 (public.events) with its cutline pinned to that
# snapshot. Idempotent: rerunning rebuilds the lake and re-registers the table.
#
# Usage:
#   test/fixture/setup.sh "<libpq-dsn>" [warehouse_dir]
#
# Example:
#   test/fixture/setup.sh "host=localhost port=54329 dbname=postgres user=postgres"
#   export TIERDB_TEST_DSN="host=localhost port=54329 dbname=postgres user=postgres"
#
# Requires: psql, and a Python with pyiceberg + pyarrow. Point $PYTHON at that
# interpreter if it is not the default python3 (e.g. PYTHON=./.venv/bin/python).
set -euo pipefail

PYTHON="${PYTHON:-python3}"

DSN="${1:-}"
if [[ -z "$DSN" ]]; then
	echo "usage: $0 \"<libpq-dsn>\" [warehouse_dir]" >&2
	exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"
CATALOG_SQL="$REPO_ROOT/sql/catalog.sql"
WAREHOUSE="${2:-$SCRIPT_DIR/.lake}"

if [[ ! -f "$CATALOG_SQL" ]]; then
	echo "error: catalog schema not found at $CATALOG_SQL" >&2
	exit 1
fi

echo "==> applying tierdb catalog schema"
psql "$DSN" -v ON_ERROR_STOP=1 -q -f "$CATALOG_SQL"

echo "==> generating Iceberg lake fixture in $WAREHOUSE"
METADATA_LOCATION="$("$PYTHON" "$SCRIPT_DIR/make_iceberg.py" "$WAREHOUSE" | tail -n 1)"
echo "    metadata_location=$METADATA_LOCATION"

echo "==> registering table 90001 (public.events) and its cutline"
psql "$DSN" -v ON_ERROR_STOP=1 -q <<SQL
DROP TABLE IF EXISTS public.events;
CREATE TABLE public.events (id bigint PRIMARY KEY, event_time bigint, val text);

DELETE FROM tierdb.tables WHERE table_id = 90001;
INSERT INTO tierdb.tables
    (table_id, schema_name, table_name, primary_key_cols, tier_key_col,
     tier_key_type, partition_scheme, lake_format, lake_table_ref, mode)
VALUES
    (90001, 'public', 'events', ARRAY['id'], 'event_time',
     'bigint', '{}'::jsonb, 'iceberg', 'warehouse.public.events', 'tiered');

DELETE FROM tierdb.cutline WHERE table_id = 90001;
INSERT INTO tierdb.cutline (table_id, tier_key_hi, lake_snapshot_id, lake_props)
VALUES (90001, 100, 1, jsonb_build_object('metadata_location', '$METADATA_LOCATION'));
SQL

echo "==> fixture ready"
echo "    export TIERDB_TEST_DSN=\"$DSN\""
