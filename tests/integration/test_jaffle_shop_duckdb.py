"""Integration test for the Jaffle Shop DuckDB example script."""

import os
import sys

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("jafgen")

import duckdb

# Add docs/examples to path so we can import the script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "docs", "examples"))

from jaffle_shop_duckdb import TABLE_NAMES, create_schema, generate_data, load_data, verify


@pytest.fixture(scope="module")
def jaffle_db(tmp_path_factory):
    """Generate 30 days of data and load into DuckDB. Shared across all tests in this module."""
    tmpdir = tmp_path_factory.mktemp("jaffle")
    data_dir = generate_data(output_dir=str(tmpdir), years=1)

    db_path = tmpdir / "test_jaffle.duckdb"
    conn = duckdb.connect(str(db_path))

    schema_path = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "examples", "jaffle_shop_schema.sql")
    create_schema(conn=conn, schema_path=schema_path)
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

    def test_fk_order_items_to_orders(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        orphans = jaffle_db.execute(
            "SELECT COUNT(*) FROM order_items WHERE order_id NOT IN (SELECT id FROM orders)"
        ).fetchone()[0]
        assert orphans == 0

    def test_fk_order_items_to_products(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        orphans = jaffle_db.execute(
            "SELECT COUNT(*) FROM order_items WHERE sku NOT IN (SELECT sku FROM products)"
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

    def test_join_order_items_to_products_and_orders(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        """Three-way join: order_items links orders to products."""
        rows = jaffle_db.execute("""
            SELECT o.id as order_id, p.name as product_name, oi.quantity
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.id
            JOIN products p ON oi.sku = p.sku
            LIMIT 10
        """).fetchall()
        assert len(rows) > 0
        assert all(qty > 0 for _, _, qty in rows)

    def test_verify_function(self, jaffle_db: duckdb.DuckDBPyConnection) -> None:
        results = verify(jaffle_db)
        assert all(count > 0 for count in results["row_counts"].values())
        assert all(count == 0 for count in results["fk_orphans"].values())
        assert len(results["revenue_by_store"]) > 0
        assert len(results["top_customers"]) > 0
