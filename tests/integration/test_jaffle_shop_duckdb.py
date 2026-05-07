"""Integration test for the Jaffle Shop DuckDB loader."""

import shutil
from pathlib import Path

import pytest

import duckdb

from slayer.demo.jaffle_shop import (
    TABLE_NAMES,
    create_schema,
    generate_data,
    load_data,
    verify,
)

# Cached 3-year demo DB maintained by tests/integration/test_notebooks.py's
# session-scoped _ensure_jaffle_db fixture. Reusing it avoids ~10s of
# `jafgen` subprocess time per pytest invocation.
_CACHED_JAFFLE_DB = (
    Path(__file__).resolve().parent.parent.parent
    / "docs" / "examples" / "jaffle_data" / "demo" / "jaffle_shop.duckdb"
)


@pytest.fixture(scope="module")
def jaffle_db(tmp_path_factory):
    """Module-scoped DuckDB connection with Jaffle Shop data. Reuses the
    project-wide cached DB at docs/examples/jaffle_data/demo/jaffle_shop.duckdb
    when present; falls back to ``jafgen`` for fresh checkouts where
    _ensure_jaffle_db has never run."""
    tmpdir = tmp_path_factory.mktemp("jaffle")
    db_path = tmpdir / "test_jaffle.duckdb"

    if _CACHED_JAFFLE_DB.exists():
        shutil.copy(_CACHED_JAFFLE_DB, db_path)
        conn = duckdb.connect(str(db_path))
    else:
        data_dir = generate_data(output_dir=str(tmpdir), years=1)
        conn = duckdb.connect(str(db_path))
        create_schema(conn=conn)
        load_data(conn=conn, data_dir=data_dir)

    yield conn
    conn.close()


@pytest.mark.integration
class TestJaffleShopDuckDB:
    def test_all_tables_populated(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        for table in TABLE_NAMES.values():
            count = jaffle_db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert count > 0, f"Table {table} is empty"

    def test_fk_orders_to_customers(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        orphans = jaffle_db.execute(
            "SELECT COUNT(*) FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)"
        ).fetchone()[0]
        assert orphans == 0

    def test_fk_orders_to_stores(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        orphans = jaffle_db.execute(
            "SELECT COUNT(*) FROM orders WHERE store_id NOT IN (SELECT id FROM stores)"
        ).fetchone()[0]
        assert orphans == 0

    def test_fk_items_to_orders(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        orphans = jaffle_db.execute(
            "SELECT COUNT(*) FROM items WHERE order_id NOT IN (SELECT id FROM orders)"
        ).fetchone()[0]
        assert orphans == 0

    def test_fk_items_to_products(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        orphans = jaffle_db.execute(
            "SELECT COUNT(*) FROM items WHERE sku NOT IN (SELECT sku FROM products)"
        ).fetchone()[0]
        assert orphans == 0

    def test_fk_supplies_to_products(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        orphans = jaffle_db.execute(
            "SELECT COUNT(*) FROM supplies WHERE sku NOT IN (SELECT sku FROM products)"
        ).fetchone()[0]
        assert orphans == 0

    def test_fk_tweets_to_customers(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        orphans = jaffle_db.execute(
            "SELECT COUNT(*) FROM tweets WHERE user_id NOT IN (SELECT id FROM customers)"
        ).fetchone()[0]
        assert orphans == 0

    def test_monetary_values_in_dollars(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        """Sanity check: values should be in dollars (single digits to hundreds), not cents."""
        max_total = jaffle_db.execute("SELECT MAX(order_total) FROM orders").fetchone()[0]
        assert max_total < 1000, f"order_total {max_total} looks like cents, not dollars"

        max_price = jaffle_db.execute("SELECT MAX(price) FROM products").fetchone()[0]
        assert max_price < 100, f"product price {max_price} looks like cents, not dollars"

    def test_join_orders_customers(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        rows = jaffle_db.execute("""
            SELECT c.name, COUNT(*) as cnt
            FROM orders o JOIN customers c ON o.customer_id = c.id
            GROUP BY c.name ORDER BY cnt DESC LIMIT 5
        """).fetchall()
        assert len(rows) > 0
        assert all(cnt > 0 for _, cnt in rows)

    def test_join_revenue_by_store(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        rows = jaffle_db.execute("""
            SELECT s.name, SUM(o.order_total) as revenue
            FROM orders o JOIN stores s ON o.store_id = s.id
            GROUP BY s.name
        """).fetchall()
        assert len(rows) > 0
        assert all(revenue > 0 for _, revenue in rows)

    def test_join_items_to_products_and_orders(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        """Three-way join: items links orders to products."""
        rows = jaffle_db.execute("""
            SELECT o.id as order_id, p.name as product_name, p.price
            FROM items oi
            JOIN orders o ON oi.order_id = o.id
            JOIN products p ON oi.sku = p.sku
            LIMIT 10
        """).fetchall()
        assert len(rows) > 0
        assert all(price > 0 for _, _, price in rows)

    def test_verify_function(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        results = verify(jaffle_db)
        assert all(count > 0 for count in results["row_counts"].values())
        assert all(count == 0 for count in results["fk_orphans"].values())
        assert len(results["revenue_by_store"]) > 0
        assert len(results["top_customers"]) > 0
