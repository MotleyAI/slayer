"""Jaffle Shop demo: the list columns products.ingredients and orders.item_skus."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import duckdb
import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.demo import jaffle_shop
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database import engine_factory
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture(autouse=True)
def _reset_engines():
    yield
    engine_factory.reset_cache()


@pytest.fixture
def demo_conn(tmp_path: Path, jafgen_dir: str):
    conn = duckdb.connect(str(tmp_path / "demo.duckdb"))
    jaffle_shop.create_schema(conn)
    jaffle_shop.load_data(conn=conn, data_dir=jafgen_dir)
    yield conn
    conn.close()


class TestLoadedLists:
    def test_ingredients_are_the_sorted_distinct_supplies(self, demo_conn: duckdb.DuckDBPyConnection) -> None:
        expected: dict[str, set[str]] = {}
        for sku, name in demo_conn.execute("SELECT sku, name FROM supplies").fetchall():
            expected.setdefault(sku, set()).add(name)
        rows = demo_conn.execute("SELECT sku, ingredients FROM products").fetchall()
        assert rows
        for sku, ingredients in rows:
            assert ingredients == sorted(expected.get(sku, set())), sku

    def test_item_skus_are_the_orders_items(self, demo_conn: duckdb.DuckDBPyConnection) -> None:
        expected: dict[str, Counter] = {}
        for order_id, sku in demo_conn.execute("SELECT order_id, sku FROM items").fetchall():
            expected.setdefault(order_id, Counter())[sku] += 1
        rows = demo_conn.execute("SELECT id, item_skus FROM orders").fetchall()
        assert rows
        for order_id, skus in rows:
            assert Counter(skus) == expected.get(order_id, Counter()), order_id

    def test_no_list_is_null(self, demo_conn: duckdb.DuckDBPyConnection) -> None:
        nulls = demo_conn.execute(
            "SELECT (SELECT count(*) FROM products WHERE ingredients IS NULL) "
            "+ (SELECT count(*) FROM orders WHERE item_skus IS NULL)"
        ).fetchone()
        assert nulls == (0,)

    def test_a_custom_schema_without_lists_still_loads(self, tmp_path: Path, jafgen_dir: str) -> None:
        schema = jaffle_shop.JAFFLE_SCHEMA_SQL.replace(",\n    ingredients VARCHAR[]", "").replace(
            ",\n    item_skus VARCHAR[]", "")
        assert "VARCHAR[]" not in schema
        schema_path = tmp_path / "schema.sql"
        schema_path.write_text(schema)
        conn = duckdb.connect(str(tmp_path / "plain.duckdb"))
        try:
            jaffle_shop.create_schema(conn, schema_path=str(schema_path))
            jaffle_shop.load_data(conn=conn, data_dir=jafgen_dir)
            row = conn.execute("SELECT count(*) FROM orders").fetchone()
        finally:
            conn.close()
        assert row is not None
        assert row[0] > 0


class TestDemoModels:
    def test_lists_ingest_as_text_lists(self, jaffle_generator, tmp_path: Path) -> None:
        store_dir = str(tmp_path / "store")
        storage = YAMLStorage(base_dir=store_dir)
        jaffle_shop.ensure_demo_datasource(storage, storage_path=store_dir, years=1)
        for model_name, column_name in (("products", "ingredients"), ("orders", "item_skus")):
            model = run_sync(storage.get_model(name=model_name, data_source=jaffle_shop.DEMO_NAME))
            assert model is not None
            column = model.get_column(column_name)
            assert column is not None
            assert (column.type, column.element_type) == (DataType.ARRAY, DataType.TEXT)
            assert column.description

    def test_element_counts_match_the_items(self, doc_demo_storage: YAMLStorage) -> None:
        engine = SlayerQueryEngine(storage=doc_demo_storage)
        try:
            per_element = engine.execute_sync({
                "source_model": "orders", "dimensions": ["item_skus.value"],
                "measures": [{"formula": "count(item_skus.*)", "name": "units"}],
            }).data
            per_item = engine.execute_sync({
                "source_model": "items", "dimensions": ["sku"],
                "measures": [{"formula": "count(*)", "name": "units"}],
            }).data
        finally:
            engine.close()
        assert per_element
        assert {r["orders.item_skus.value"]: r["orders.units"] for r in per_element} == {
            r["items.sku"]: r["items.units"] for r in per_item}
