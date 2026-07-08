#!/usr/bin/env python3
"""Generate the Iceberg lake fixture the tierdb SQLLogicTests read from.

Writes a single snapshot holding the cold rows (id 1 and 4, both below the
cutline T=100) into a local file:// warehouse and prints the resulting
metadata_location on the last line of stdout so setup.sh can pin the cutline
to it.

Usage:
    make_iceberg.py <warehouse_dir>

Requires: pyiceberg, pyarrow (pip install "pyiceberg[sql-sqlite]" pyarrow).
"""

import os
import shutil
import sys

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    warehouse = os.path.abspath(sys.argv[1])
    shutil.rmtree(warehouse, ignore_errors=True)
    os.makedirs(warehouse, exist_ok=True)

    catalog = SqlCatalog(
        "local",
        uri=f"sqlite:///{warehouse}/catalog.db",
        warehouse=f"file://{warehouse}",
    )

    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("event_time", pa.int64()),
            ("val", pa.string()),
        ]
    )

    catalog.create_namespace_if_not_exists("public")
    table = catalog.create_table("public.events", schema=schema)

    # Cold rows that live in the lake, all below the fixture cutline T=100.
    table.append(
        pa.table(
            {
                "id": pa.array([1, 4], pa.int64()),
                "event_time": pa.array([50, 80], pa.int64()),
                "val": pa.array(["lake-1", "lake-4-original"], pa.string()),
            }
        )
    )
    table.refresh()
    print(table.metadata_location)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
