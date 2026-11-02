"""Jaffle Shop demo: order status and the store region hierarchy (demo/jaffle-shop)."""

from __future__ import annotations

import csv
import hashlib
import shutil
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from slayer.async_utils import run_sync
from slayer.core.errors import SlayerError
from slayer.demo import jaffle_shop
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage

REGION_OF_STORE = {
    "Philadelphia": "Northeast",
    "Brooklyn": "Northeast",
    "Chicago": "Midwest",
    "San Francisco": "West",
    "Los Angeles": "West",
    "New Orleans": "South",
}
REGIONS = {"Northeast", "Midwest", "West", "South"}
STATUSES = {"completed", "refunded", "cancelled"}


@pytest.fixture(autouse=True)
def _reset_engines():
    yield
    engine_factory.reset_cache()


def _loaded(db_path: Path, data_dir: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(db_path))
    jaffle_shop.create_schema(conn)
    jaffle_shop.load_data(conn=conn, data_dir=data_dir)
    return conn


@pytest.fixture
def demo_conn(tmp_path: Path, jafgen_dir: str):
    conn = _loaded(tmp_path / "demo.duckdb", jafgen_dir)
    yield conn
    conn.close()


def _sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _rows(storage: YAMLStorage, query: dict) -> list[dict]:
    return SlayerQueryEngine(storage=storage).execute_sync(query).data


def _count_key(row: dict) -> str:
    return next(k for k in row if k.endswith("_count"))


def _dim_key(row: dict) -> str:
    return next(k for k in row if not k.endswith("_count"))


class TestSchema:
    def test_regions_table_status_and_region_id_are_declared(self) -> None:
        conn = duckdb.connect()
        try:
            jaffle_shop.create_schema(conn)
            nullable = dict(
                ((t, c), n) for t, c, n in conn.execute(
                    "SELECT table_name, column_name, is_nullable FROM duckdb_columns() WHERE schema_name = 'main'"
                ).fetchall()
            )
            fks = [
                text for (text,) in conn.execute(
                    "SELECT constraint_text FROM duckdb_constraints() "
                    "WHERE table_name = 'stores' AND constraint_type = 'FOREIGN KEY'"
                ).fetchall()
            ]
        finally:
            conn.close()
        assert {c for t, c in nullable if t == "regions"} == {"id", "name"}
        assert nullable[("stores", "region_id")] is False
        assert nullable[("orders", "status")] is False
        assert any("region_id" in text and "regions" in text for text in fks)
        assert "regions" in jaffle_shop.TABLE_NAMES


class TestLoader:
    def test_regions_are_seeded(self, demo_conn: duckdb.DuckDBPyConnection) -> None:
        names = {name for (name,) in demo_conn.execute("SELECT name FROM regions").fetchall()}
        assert names == REGIONS

    def test_every_store_gets_its_mapped_region(self, demo_conn: duckdb.DuckDBPyConnection) -> None:
        rows = demo_conn.execute(
            "SELECT s.name, r.name FROM stores s LEFT JOIN regions r ON s.region_id = r.id"
        ).fetchall()
        assert rows
        assert {store: region for store, region in rows} == {store: REGION_OF_STORE[store] for store, _ in rows}

    def test_every_order_has_a_known_status(self, demo_conn: duckdb.DuckDBPyConnection) -> None:
        statuses = {s for (s,) in demo_conn.execute("SELECT DISTINCT status FROM orders").fetchall()}
        assert statuses
        assert statuses <= STATUSES

    def test_refunded_and_cancelled_orders_keep_their_amounts(
        self, demo_conn: duckdb.DuckDBPyConnection, jafgen_dir: str,
    ) -> None:
        with Path(jafgen_dir, "raw_orders.csv").open(newline="") as f:
            raw = {
                row["id"]: tuple(Decimal(row[c]) / 100 for c in ("subtotal", "tax_paid", "order_total"))
                for row in csv.DictReader(f)
            }
        loaded = demo_conn.execute(
            "SELECT id, status, subtotal, tax_paid, order_total FROM orders WHERE status <> 'completed'"
        ).fetchall()
        assert {status for _, status, *_ in loaded} == {"refunded", "cancelled"}
        for order_id, _, *amounts in loaded:
            assert tuple(amounts) == raw[order_id], order_id

    def test_status_is_fixed_by_order_id(self, tmp_path: Path, jafgen_dir: str) -> None:
        maps = []
        for name in ("a.duckdb", "b.duckdb"):
            conn = _loaded(tmp_path / name, jafgen_dir)
            try:
                maps.append(dict(conn.execute("SELECT id, status FROM orders").fetchall()))
            finally:
                conn.close()
        assert maps[0] == maps[1]

    def test_verify_reports_store_region_orphans(self, demo_conn: duckdb.DuckDBPyConnection) -> None:
        report = jaffle_shop.verify(demo_conn)
        assert report["fk_orphans"]["stores->regions"] == 0
        assert report["row_counts"]["regions"] == len(REGIONS)


class TestUnassignedStore:
    def test_unmapped_store_fails_setup_and_keeps_the_existing_db(
        self, tmp_path: Path, jafgen_dir: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        bad_dir = tmp_path / "jafgen_bad"
        shutil.copytree(jafgen_dir, bad_dir)
        stores_csv = bad_dir / "raw_stores.csv"
        with stores_csv.open(newline="") as f:
            rows = list(csv.DictReader(f))
        rows[0]["name"] = "Atlantis"
        with stores_csv.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        monkeypatch.setattr(jaffle_shop, "generate_data", lambda *args, **kwargs: str(bad_dir))

        store_dir = str(tmp_path / "store")
        db_path = jaffle_shop.resolve_demo_db_path(store_dir)
        stale = duckdb.connect(db_path)
        try:
            stale.execute("CREATE TABLE orders (id VARCHAR PRIMARY KEY)")
        finally:
            stale.close()
        before = _sha256(db_path)

        with pytest.raises(SlayerError, match="Atlantis"):
            jaffle_shop.ensure_demo_datasource(YAMLStorage(base_dir=store_dir), storage_path=store_dir, years=1)
        assert _sha256(db_path) == before


class TestDemoModels:
    def test_setup_ingests_regions_status_and_the_store_region_join(
        self, jaffle_generator, tmp_path: Path,
    ) -> None:
        store_dir = str(tmp_path / "store")
        storage = YAMLStorage(base_dir=store_dir)
        jaffle_shop.ensure_demo_datasource(storage, storage_path=store_dir, years=1)
        names = set(run_sync(storage.list_models(data_source=jaffle_shop.DEMO_NAME)))
        assert "regions" in names
        orders = run_sync(storage.get_model(name="orders", data_source=jaffle_shop.DEMO_NAME))
        stores = run_sync(storage.get_model(name="stores", data_source=jaffle_shop.DEMO_NAME))
        assert orders is not None and stores is not None
        assert "status" in {c.name for c in orders.columns}
        assert "regions" in {j.target_model for j in stores.joins}


class TestTwoYearDemo:
    def test_all_three_statuses_occur(self, doc_demo_storage: YAMLStorage) -> None:
        rows = _rows(doc_demo_storage, {"source_model": "orders", "measures": ["count(*)"], "dimensions": ["status"]})
        assert rows
        assert {row[_dim_key(row)] for row in rows} == STATUSES

    def test_region_is_a_two_hop_dimension_of_orders(self, doc_demo_storage: YAMLStorage) -> None:
        rows = _rows(
            doc_demo_storage,
            {"source_model": "orders", "measures": ["count(*)"], "dimensions": ["stores.regions.name"]},
        )
        total = _rows(doc_demo_storage, {"source_model": "orders", "measures": ["count(*)"]})
        assert rows
        regions = [row[_dim_key(row)] for row in rows]
        assert len(regions) == len(set(regions))
        assert set(regions) <= REGIONS
        assert sum(row[_count_key(row)] for row in rows) == total[0][_count_key(total[0])]
