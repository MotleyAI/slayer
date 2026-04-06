#!/usr/bin/env python3
"""Generate Jaffle Shop data and load it into DuckDB with proper relationships.

Dependencies: pip install jafgen duckdb
Usage: python jaffle_shop_duckdb.py
"""

import os
import subprocess
import sys
import tempfile

import duckdb

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "jaffle_shop_schema.sql")

# Tables with monetary columns stored in cents in the CSV, mapped to their cent-columns
CENTS_COLUMNS = {
    "orders": ["subtotal", "tax_paid", "order_total"],
    "products": ["price"],
    "supplies": ["cost"],
}

# CSV file name (without prefix/suffix) -> DuckDB table name
TABLE_NAMES = {
    "customers": "customers",
    "stores": "stores",
    "products": "products",
    "orders": "orders",
    "order_items": "order_items",
    "supplies": "supplies",
    "tweets": "tweets",
}

# Load order: independent tables first, then dependent ones
LOAD_ORDER = ["customers", "stores", "products", "orders", "order_items", "supplies", "tweets"]


def generate_data(output_dir: str, years: int = 3, days: int = 0) -> str:
    """Run jafgen to generate CSV data into output_dir. Returns the jaffle-data path."""
    cmd = ["jafgen", str(years)]
    if days > 0:
        cmd.extend(["--days", str(days)])
    subprocess.run(cmd, cwd=output_dir, check=True)
    return os.path.join(output_dir, "jaffle-data")


def create_schema(conn: duckdb.DuckDBPyConnection, schema_path: str = SCHEMA_FILE) -> None:
    """Create tables from the SQL schema file."""
    with open(schema_path) as f:
        sql = f.read()
    # Strip SQL comments before splitting on semicolons
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    clean_sql = "\n".join(lines)
    for statement in clean_sql.split(";"):
        statement = statement.strip()
        if statement:
            conn.execute(statement)


def load_data(conn: duckdb.DuckDBPyConnection, data_dir: str, prefix: str = "raw") -> None:
    """Load CSV files into DuckDB tables, converting cents to dollars for monetary columns."""
    for csv_name in LOAD_ORDER:
        table = TABLE_NAMES[csv_name]
        csv_path = os.path.join(data_dir, f"{prefix}_{csv_name}.csv")
        if not os.path.exists(csv_path):
            print(f"  Skipping {table}: {csv_path} not found")
            continue

        cents_cols = CENTS_COLUMNS.get(csv_name, [])
        if cents_cols:
            # Read into temp view, transform cents to dollars, insert
            conn.execute(f"CREATE TEMP VIEW _{table}_raw AS SELECT * FROM read_csv_auto('{csv_path}')")
            columns = [row[0] for row in conn.execute(f"DESCRIBE _{table}_raw").fetchall()]
            select_parts = []
            for col in columns:
                if col in cents_cols:
                    select_parts.append(f"CAST({col} AS DOUBLE) / 100.0 AS {col}")
                else:
                    select_parts.append(col)
            conn.execute(f"INSERT INTO {table} SELECT {', '.join(select_parts)} FROM _{table}_raw")
            conn.execute(f"DROP VIEW _{table}_raw")
        else:
            conn.execute(f"INSERT INTO {table} SELECT * FROM read_csv_auto('{csv_path}')")

        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count} rows")


def verify(conn: duckdb.DuckDBPyConnection) -> dict:
    """Run verification queries. Returns dict of results."""
    results = {}

    # Row counts
    counts = {}
    for table in TABLE_NAMES.values():
        counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    results["row_counts"] = counts

    # FK integrity checks: count orphaned records (should all be 0)
    fk_checks = {
        "orders->customers": "SELECT COUNT(*) FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)",
        "orders->stores": "SELECT COUNT(*) FROM orders WHERE store_id NOT IN (SELECT id FROM stores)",
        "order_items->orders": "SELECT COUNT(*) FROM order_items WHERE order_id NOT IN (SELECT id FROM orders)",
        "order_items->products": "SELECT COUNT(*) FROM order_items WHERE sku NOT IN (SELECT sku FROM products)",
        "supplies->products": "SELECT COUNT(*) FROM supplies WHERE sku NOT IN (SELECT sku FROM products)",
        "tweets->customers": "SELECT COUNT(*) FROM tweets WHERE user_id NOT IN (SELECT id FROM customers)",
    }
    orphans = {}
    for name, query in fk_checks.items():
        orphans[name] = conn.execute(query).fetchone()[0]
    results["fk_orphans"] = orphans

    # Revenue by store
    results["revenue_by_store"] = conn.execute("""
        SELECT s.name, COUNT(*) as order_count, SUM(o.order_total) as revenue
        FROM orders o JOIN stores s ON o.store_id = s.id
        GROUP BY s.name ORDER BY revenue DESC
    """).fetchall()

    # Top 5 customers by order count
    results["top_customers"] = conn.execute("""
        SELECT c.name, COUNT(*) as order_count
        FROM orders o JOIN customers c ON o.customer_id = c.id
        GROUP BY c.name ORDER BY order_count DESC LIMIT 5
    """).fetchall()

    return results


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="jaffle_shop_") as tmpdir:
        # Generate data
        print("=== Generating 3 years of Jaffle Shop data ===")
        data_dir = generate_data(tmpdir, years=3)

        # Create DuckDB and load
        db_path = os.path.join(tmpdir, "jaffle_shop.duckdb")
        conn = duckdb.connect(db_path)

        print("\n=== Creating schema ===")
        create_schema(conn)

        print("\n=== Loading data ===")
        load_data(conn, data_dir)

        # Verify
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

        conn.close()

        if not all_ok:
            print("\nFAILED: FK integrity violations found!")
            sys.exit(1)

        print(f"\nDone! Database was at: {db_path}")


if __name__ == "__main__":
    main()
