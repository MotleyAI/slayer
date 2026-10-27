"""Jaffle Shop demo: keys and joins on any SQLAlchemy, exact money, stale-DB rebuild, self-healing setup."""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.demo import jaffle_shop
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage

MONEY_COLUMNS = {
    ("orders", "subtotal"),
    ("orders", "tax_paid"),
    ("orders", "order_total"),
    ("products", "price"),
    ("supplies", "cost"),
}

EXPECTED_EDGES = {
    ("items", "orders"),
    ("items", "products"),
    ("orders", "customers"),
    ("orders", "stores"),
    ("supplies", "products"),
    ("tweets", "customers"),
}

README_QUERY = {
    "source_model": "orders",
    "measures": ["sum(order_total)", "change_pct(sum(order_total))"],
    "dimensions": ["stores.name", "month(ordered_at)"],
}

# The bundled schema as shipped in 1.4.1: money stored as DOUBLE.
V141_SCHEMA_SQL = """\
CREATE TABLE customers (id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL);
CREATE TABLE stores (
    id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, opened_at DATE NOT NULL, tax_rate DOUBLE NOT NULL
);
CREATE TABLE products (
    sku VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, type VARCHAR NOT NULL,
    price DOUBLE NOT NULL, description VARCHAR NOT NULL
);
CREATE TABLE orders (
    id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL REFERENCES customers(id),
    ordered_at DATE NOT NULL,
    store_id VARCHAR NOT NULL REFERENCES stores(id),
    subtotal DOUBLE NOT NULL, tax_paid DOUBLE NOT NULL, order_total DOUBLE NOT NULL
);
CREATE TABLE items (
    id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL REFERENCES orders(id),
    sku VARCHAR NOT NULL REFERENCES products(sku)
);
CREATE TABLE supplies (
    id VARCHAR NOT NULL, name VARCHAR NOT NULL, cost DOUBLE NOT NULL, perishable VARCHAR NOT NULL,
    sku VARCHAR NOT NULL REFERENCES products(sku),
    PRIMARY KEY (id, sku)
);
CREATE TABLE tweets (
    id VARCHAR PRIMARY KEY,
    user_id VARCHAR NOT NULL REFERENCES customers(id),
    tweeted_at DATE NOT NULL,
    content VARCHAR NOT NULL
);
"""

REGCLASS_ERROR = "Catalog Error: Type with name REGCLASS does not exist!"


@pytest.fixture
def store_dir(tmp_path: Path) -> str:
    return str(tmp_path / "store")


@pytest.fixture
def storage(store_dir: str) -> YAMLStorage:
    return YAMLStorage(base_dir=store_dir)


@pytest.fixture
def sa_without_generic_key_reflection(monkeypatch: pytest.MonkeyPatch) -> None:
    """SQLAlchemy >= 2.0.55 on DuckDB: the inherited Postgres key queries raise."""

    def _raise(*args: object, **kwargs: object) -> None:
        raise sa.exc.ProgrammingError(statement="SELECT ... ::REGCLASS", params=None, orig=Exception(REGCLASS_ERROR))

    for name in ("get_pk_constraint", "get_foreign_keys", "get_unique_constraints"):
        monkeypatch.setattr(Inspector, name, _raise)


def _setup(storage: YAMLStorage, store_dir: str) -> tuple:
    return jaffle_shop.ensure_demo_datasource(storage, storage_path=store_dir, years=1)


def _stored_models(storage: YAMLStorage) -> dict[str, SlayerModel]:
    names = run_sync(storage.list_models(data_source=jaffle_shop.DEMO_NAME))
    return {
        name: run_sync(storage.get_model(name=name, data_source=jaffle_shop.DEMO_NAME))
        for name in names
    }


def _edges(models: dict[str, SlayerModel]) -> set[tuple[str, str]]:
    return {(name, j.target_model) for name, m in models.items() for j in m.joins}


def _column(model: SlayerModel, name: str) -> Column:
    return next(c for c in model.columns if c.name == name)


def _db_path(store_dir: str) -> str:
    return jaffle_shop.resolve_demo_db_path(store_dir)


def _open_direct(db_path: str) -> duckdb.DuckDBPyConnection:
    """A raw connection; cached engines are released first (DuckDB refuses mixed configurations)."""
    engine_factory.reset_cache()
    return duckdb.connect(db_path)


def _live_types(db_path: str) -> dict[tuple[str, str], str]:
    conn = _open_direct(db_path)
    try:
        rows = conn.execute(
            "SELECT table_name, column_name, data_type FROM duckdb_columns() WHERE schema_name = 'main'"
        ).fetchall()
    finally:
        conn.close()
    return {(t, c): dt for t, c, dt in rows}


def _build_v141_db(*, db_path: str, data_dir: str, tmp_path: Path) -> None:
    schema_file = tmp_path / "v141_schema.sql"
    schema_file.write_text(V141_SCHEMA_SQL)
    conn = duckdb.connect(db_path)
    try:
        jaffle_shop.create_schema(conn, schema_path=str(schema_file))
        jaffle_shop.load_data(conn=conn, data_dir=data_dir)
    finally:
        conn.close()


def _register_demo_ds(*, storage: YAMLStorage, store_dir: str) -> DatasourceConfig:
    """The demo datasource record, saved without running demo setup."""
    ds = DatasourceConfig(name=jaffle_shop.DEMO_NAME, type="duckdb", database=_db_path(store_dir))
    run_sync(storage.save_datasource(ds))
    return ds


def _store_affected_models(*, storage: YAMLStorage, store_dir: str) -> None:
    """Persist demo models as an affected release left them: no joins, no money ``db_type``."""
    ds = _register_demo_ds(storage=storage, store_dir=store_dir)
    for model in ingest_datasource(datasource=ds):
        jaffle_shop.apply_demo_enrichment(model)
        model.joins = []
        for column in model.columns:
            if (model.name, column.name) in MONEY_COLUMNS:
                column.db_type = None
        run_sync(storage.save_model(model))


def _strip_joins_and_edit(storage: YAMLStorage) -> None:
    for model in _stored_models(storage).values():
        model.joins = []
        if model.name == "orders":
            _column(model, "order_total").description = "My own total"
        run_sync(storage.save_model(model))


def _run(storage: YAMLStorage, query: dict) -> list[dict]:
    return SlayerQueryEngine(storage=storage).execute_sync(query).data


@pytest.fixture(autouse=True)
def _reset_engines():
    yield
    engine_factory.reset_cache()


# ---------------------------------------------------------------------------
# Keys and joins (models/fk-ingestion: DuckDB demo keeps its joins)
# ---------------------------------------------------------------------------


class TestDemoKeysAndJoins:
    def _assert_keys_and_joins(self, storage: YAMLStorage) -> None:
        models = _stored_models(storage)
        assert _edges(models) == EXPECTED_EDGES
        pks = {name: {c.name for c in m.columns if c.primary_key} for name, m in models.items()}
        assert pks == {
            "customers": {"id"},
            "stores": {"id"},
            "products": {"sku"},
            "orders": {"id"},
            "items": {"id"},
            "supplies": {"id", "sku"},
            "tweets": {"id"},
        }
        assert _run(storage, README_QUERY)

    def test_demo_ingests_its_joins_and_keys(self, jaffle_generator, storage, store_dir) -> None:
        _setup(storage, store_dir)
        self._assert_keys_and_joins(storage)

    def test_demo_keeps_joins_when_generic_key_reflection_raises(
        self, jaffle_generator, storage, store_dir, sa_without_generic_key_reflection,
    ) -> None:
        _setup(storage, store_dir)
        self._assert_keys_and_joins(storage)


# ---------------------------------------------------------------------------
# Exact money (demo/jaffle-shop: Demo money is exact)
# ---------------------------------------------------------------------------


class TestDemoMoneyIsExact:
    def test_money_columns_are_decimal_12_2(self, jaffle_generator, storage, store_dir) -> None:
        _setup(storage, store_dir)
        types = _live_types(_db_path(store_dir))
        for key in MONEY_COLUMNS:
            assert types[key] == "DECIMAL(12,2)", key
        assert types[("stores", "tax_rate")] == "DOUBLE"

    def test_money_converted_exactly_from_cents(self, jaffle_generator, jafgen_dir, storage, store_dir) -> None:
        _setup(storage, store_dir)
        raw = duckdb.connect()
        try:
            cents = raw.execute(
                "SELECT sum(order_total) FROM read_csv_auto(?)",
                [os.path.join(jafgen_dir, "raw_orders.csv")],
            ).fetchone()[0]
        finally:
            raw.close()
        conn = _open_direct(_db_path(store_dir))
        try:
            stored = conn.execute("SELECT sum(order_total) FROM orders").fetchone()[0]
        finally:
            conn.close()
        assert isinstance(stored, Decimal)
        assert stored == Decimal(int(cents)) / 100

    def test_revenue_by_store_and_month_is_exact(self, jaffle_generator, storage, store_dir) -> None:
        _setup(storage, store_dir)
        rows = _run(storage, {
            "source_model": "orders",
            "measures": ["sum(order_total)"],
            "dimensions": ["stores.name", "month(ordered_at)"],
        })
        assert rows
        value_key = next(k for k in rows[0] if "order_total" in k)
        values = [row[value_key] for row in rows]
        assert all(isinstance(v, Decimal) for v in values)
        conn = _open_direct(_db_path(store_dir))
        try:
            expected = [
                r[0] for r in conn.execute(
                    "SELECT sum(o.order_total) FROM orders o JOIN stores s ON o.store_id = s.id "
                    "GROUP BY s.name, date_trunc('month', o.ordered_at)"
                ).fetchall()
            ]
        finally:
            conn.close()
        assert sorted(values) == sorted(expected)

    def test_demo_models_declare_decimal_db_type(self, jaffle_generator, storage, store_dir) -> None:
        _setup(storage, store_dir)
        models = _stored_models(storage)
        for table, column in MONEY_COLUMNS:
            assert _column(models[table], column).db_type == "DECIMAL(12,2)", (table, column)

    def test_enrichment_fills_only_an_empty_db_type(self) -> None:
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source=jaffle_shop.DEMO_NAME,
            columns=[
                Column(name="order_total", type=DataType.DOUBLE),
                Column(name="subtotal", type=DataType.DOUBLE, db_type="NUMERIC(18,4)"),
            ],
        )
        jaffle_shop.apply_demo_enrichment(model)
        assert _column(model, "order_total").db_type == "DECIMAL(12,2)"
        assert _column(model, "subtotal").db_type == "NUMERIC(18,4)"

    def test_user_set_db_type_survives_setup(self, jaffle_generator, storage, store_dir) -> None:
        _setup(storage, store_dir)
        orders = _stored_models(storage)["orders"]
        _column(orders, "subtotal").db_type = "NUMERIC(18,4)"
        run_sync(storage.save_model(orders))
        _setup(storage, store_dir)
        assert _column(_stored_models(storage)["orders"], "subtotal").db_type == "NUMERIC(18,4)"


# ---------------------------------------------------------------------------
# Rebuild and healing (demo/jaffle-shop: stale DB rebuilt; setup heals models)
# ---------------------------------------------------------------------------


class TestStaleDemoDatabase:
    def test_float_money_db_is_rebuilt(self, jaffle_generator, jafgen_dir, storage, store_dir, tmp_path) -> None:
        _build_v141_db(db_path=_db_path(store_dir), data_dir=jafgen_dir, tmp_path=tmp_path)
        _setup(storage, store_dir)
        assert _live_types(_db_path(store_dir))[("orders", "order_total")] == "DECIMAL(12,2)"
        assert jaffle_generator.calls == 1

    def test_up_to_date_db_is_reused(self, jaffle_generator, storage, store_dir) -> None:
        _setup(storage, store_dir)
        stat = os.stat(_db_path(store_dir))
        _setup(storage, store_dir)
        assert jaffle_generator.calls == 1
        assert os.stat(_db_path(store_dir)).st_ino == stat.st_ino

    def test_rebuild_while_factory_engine_is_open(
        self, jaffle_generator, jafgen_dir, storage, store_dir, tmp_path,
    ) -> None:
        db_path = _db_path(store_dir)
        _build_v141_db(db_path=db_path, data_dir=jafgen_dir, tmp_path=tmp_path)
        ds = _register_demo_ds(storage=storage, store_dir=store_dir)
        held = engine_factory.get_engine(ds)
        with held.connect() as conn:
            conn.exec_driver_sql("SELECT count(*) FROM orders").fetchall()
        _setup(storage, store_dir)
        assert jaffle_generator.calls == 1
        with engine_factory.get_engine(ds).connect() as conn:
            total = conn.exec_driver_sql("SELECT sum(order_total) FROM orders").scalar()
        assert isinstance(total, Decimal)

    def test_file_held_by_another_process_fails_with_one_line_naming_it(
        self, jaffle_generator, jafgen_dir, storage, store_dir, tmp_path,
    ) -> None:
        db_path = _db_path(store_dir)
        _build_v141_db(db_path=db_path, data_dir=jafgen_dir, tmp_path=tmp_path)
        holder = subprocess.Popen(
            [sys.executable, "-c", textwrap.dedent(f"""
                import sys, duckdb
                conn = duckdb.connect({db_path!r})
                print("locked", flush=True)
                sys.stdin.read()
            """)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        try:
            assert holder.stdout.readline().strip() == "locked"
            with pytest.raises(Exception) as excinfo:
                _setup(storage, store_dir)
        finally:
            holder.communicate(input="")
        message = str(excinfo.value)
        assert db_path in message
        assert "\n" not in message.strip()
        assert _live_types(db_path)[("orders", "order_total")] == "DOUBLE"


class TestDemoSetupHealsModels:
    def test_joinless_models_regain_joins_without_rebuild(self, jaffle_generator, storage, store_dir) -> None:
        _setup(storage, store_dir)
        _strip_joins_and_edit(storage)
        _setup(storage, store_dir)
        models = _stored_models(storage)
        assert jaffle_generator.calls == 1
        assert _edges(models) == EXPECTED_EDGES
        assert _column(models["orders"], "order_total").description == "My own total"
        assert _run(storage, README_QUERY)

    def test_joinless_models_regain_joins_with_rebuild(
        self, jaffle_generator, jafgen_dir, storage, store_dir, tmp_path,
    ) -> None:
        _build_v141_db(db_path=_db_path(store_dir), data_dir=jafgen_dir, tmp_path=tmp_path)
        _store_affected_models(storage=storage, store_dir=store_dir)
        _strip_joins_and_edit(storage)
        _setup(storage, store_dir)
        models = _stored_models(storage)
        assert jaffle_generator.calls == 1
        assert _edges(models) == EXPECTED_EDGES
        assert _column(models["orders"], "order_total").description == "My own total"
        for table, column in MONEY_COLUMNS:
            assert _column(models[table], column).db_type == "DECIMAL(12,2)", (table, column)
        assert _run(storage, README_QUERY)
