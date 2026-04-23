#!/usr/bin/env python3
"""Generate Jaffle Shop data and load it into DuckDB.

This script is a thin wrapper around ``slayer.demo.jaffle_shop``, which is the
single source of truth for the Jaffle Shop schema and setup. Keeping this file
so ``docs/examples/jaffle_data/`` stays self-contained for tutorial runs.

Dependencies: pip install jafgen duckdb
Usage: python ingest_jaffle_shop.py
"""

import os
import sys
import tempfile

import duckdb

from slayer.demo.jaffle_shop import (
    CENTS_COLUMNS,
    LOAD_ORDER,
    TABLE_NAMES,
    create_schema,
    generate_data,
    load_data,
    verify,
)

__all__ = [
    "CENTS_COLUMNS",
    "LOAD_ORDER",
    "SCHEMA_FILE",
    "TABLE_NAMES",
    "create_schema",
    "generate_data",
    "load_data",
    "verify",
]

# Legacy constant kept so existing callers that pass ``schema_path=SCHEMA_FILE``
# continue to work — ``create_schema`` accepts the file but defaults to the
# inlined ``JAFFLE_SCHEMA_SQL`` when not provided.
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "jaffle_shop_schema.sql")
if not os.path.exists(SCHEMA_FILE):
    SCHEMA_FILE = None  # type: ignore[assignment]


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="jaffle_shop_") as tmpdir:
        print("=== Generating 3 years of Jaffle Shop data ===")
        data_dir = generate_data(output_dir=tmpdir, years=3)

        db_path = os.path.join(tmpdir, "jaffle_shop.duckdb")
        conn = duckdb.connect(db_path)
        try:
            print("\n=== Creating schema ===")
            create_schema(conn)

            print("\n=== Loading data ===")
            load_data(conn=conn, data_dir=data_dir)

            print("\n=== Verification ===")
            results = verify(conn)

            print("\nRow counts:")
            for table, count in results["row_counts"].items():
                print(f"  {table}: {count}")

            print("\nFK integrity (orphaned records, should all be 0):")
            all_ok = True
            for fk, count in results["fk_orphans"].items():
                status = "OK" if count == 0 else f"FAIL ({count} orphans)"
                if count > 0:
                    all_ok = False
                print(f"  {fk}: {status}")

            print("\nRevenue by store:")
            for name, order_count, revenue in results["revenue_by_store"]:
                print(f"  {name}: {order_count} orders, ${revenue:,.2f}")

            print("\nTop 5 customers by order count:")
            for name, order_count in results["top_customers"]:
                print(f"  {name}: {order_count} orders")
        finally:
            conn.close()

        if not all_ok:
            print("\nFAILED: FK integrity violations found!")
            sys.exit(1)

        print(f"\nDone! Database was at: {db_path}")


if __name__ == "__main__":
    main()
