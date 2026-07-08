# tierdb SQLLogicTests

Integration tests for the `tierdb` storage extension. They attach a real
Postgres catalog and an Iceberg lake fixture, then exercise the merged read
path, DML routing across the tier seam, and write atomicity.

## What they cover

| File | Scenarios |
|------|-----------|
| `sql/tierdb_smoke.test` | Attach + a three-way merged read + `count(*)`. |
| `sql/tierdb_read.test`  | Hot/cold/delta merge, delta upsert override, tombstone hide, delta-only cold tier, projection pushdown. |
| `sql/tierdb_write.test` | INSERT/UPDATE/DELETE routing across the seam, retention-line rejection, mirrored mode heap-only writes. |
| `sql/tierdb_txn.test`   | ROLLBACK discards, COMMIT is atomic, autocommit read-your-own-write. |
| `sql/tierdb_types.test` | Column type round-trips (text, int, bool, double, numeric, uuid, date, timestamp, timestamptz, json, and `integer[]`/`text[]` arrays) through the heap and the delta overlay, on INSERT/UPDATE/DELETE. |

The tests are gated on `TIERDB_TEST_DSN`; without it they are skipped, so a
plain `make test` on a machine with no fixture stays green.

## Running

1. Stand up the fixture (Postgres with the tierdb catalog + an Iceberg lake).
   `setup.sh` applies `sql/catalog.sql`, generates the lake snapshot, and
   registers table `90001` (`public.events`) with its cutline pinned to it.

   ```sh
   # Requires psql and a Python with pyiceberg + pyarrow.
   PYTHON=/path/to/venv/bin/python \
     test/fixture/setup.sh "host=localhost port=54329 dbname=postgres user=postgres"
   ```

2. Point the tests at that DSN and run them:

   ```sh
   export TIERDB_TEST_DSN="host=localhost port=54329 dbname=postgres user=postgres"
   make test                      # release build, no sanitizer
   ```

   Against a debug build (ASan) add `ASAN_OPTIONS=detect_container_overflow=0`
   to silence the known false positive from the prebuilt, non-instrumented
   `postgres_scanner`:

   ```sh
   ASAN_OPTIONS=detect_container_overflow=0 \
     ./build/debug/test/unittest "test/sql/*"
   ```

## Notes

- Each `.test` seeds its own preconditions and cleans up, so files are
  independent and order does not matter.
- The lake fixture is regenerated under `test/fixture/.lake/` (gitignored).
- The tests reach the same Postgres twice: as `tdb` (`TYPE tierdb`, the code
  under test) and as `pg` (`TYPE postgres`) to seed rows and assert what
  actually landed in the heap versus the delta overlay.
- Type-mapping limitations: single-dimension arrays of any element type work
  (`text[]`, `numeric[]`, `boolean[]`, `timestamp[]`, ...), but Postgres reports
  multi-dimensional arrays (`integer[][]`) as plain `integer[]`, so their nesting
  cannot be recovered and they are rejected at write time. `json`/`jsonb`
  round-trip by value but surface as `VARCHAR`; a bare `numeric` (no precision)
  or one wider than 38 digits falls back to `DOUBLE`; `bytea` is untested.
  Rendering a `timestamptz` as text needs the `icu` extension, so
  `tierdb_types.test` checks it by equality instead.
