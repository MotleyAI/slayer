"""Ingestion reads every declared key on DuckDB (and keeps base behaviour on SQLite), via the public entry points."""

from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, NoReturn

import duckdb
import pytest
import sqlalchemy as sa
from sqlalchemy import event, exc
from pydantic import BaseModel
from sqlalchemy.engine.reflection import Inspector

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.engine.ingestion import (
    IngestionScanReport,
    ingest_datasource_report,
    introspect_table_to_model,
)
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import disposable_engine

_REGCLASS = "Catalog Error: Type with name REGCLASS does not exist!"
_TO_ONE = {JoinCardinality.MANY_TO_ONE, JoinCardinality.ONE_TO_ONE}


def _duck(path: Path, statements: list[str]) -> None:
    con = duckdb.connect(str(path))
    try:
        for stmt in statements:
            con.execute(stmt)
    finally:
        con.close()


@pytest.fixture(scope="module")
def keys_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    path = tmp_path_factory.mktemp("duck_keys") / "keys.duckdb"
    _duck(path, [
        'CREATE TABLE customers(id INTEGER PRIMARY KEY, code VARCHAR UNIQUE, a INTEGER, b INTEGER, '
        '"Mixed Case" INTEGER, name VARCHAR, UNIQUE (a, b))',
        'CREATE UNIQUE INDEX ux_mixed ON customers ("Mixed Case")',
        "CREATE UNIQUE INDEX ux_lower ON customers (lower(name))",
        "CREATE TABLE orders(id INTEGER PRIMARY KEY, customer_code VARCHAR REFERENCES customers(code), "
        "amount DECIMAL(12,2))",
        "CREATE TABLE pairs(id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, "
        "FOREIGN KEY (a, b) REFERENCES customers(a, b))",
        "CREATE TABLE lines(order_id INTEGER REFERENCES orders(id), n INTEGER, PRIMARY KEY (order_id, n))",
    ])
    return path


@pytest.fixture(scope="module")
def schema_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Composite keys inside ``ofr``; ``main`` holds same-named tables with other keys."""
    path = tmp_path_factory.mktemp("duck_schema") / "sch.duckdb"
    _duck(path, [
        "CREATE SCHEMA ofr",
        "CREATE TABLE ofr.parent(a INTEGER, b INTEGER, label VARCHAR, PRIMARY KEY (a, b))",
        "CREATE TABLE ofr.child(id INTEGER PRIMARY KEY, pa INTEGER, pb INTEGER, "
        "FOREIGN KEY (pa, pb) REFERENCES ofr.parent(a, b))",
        "CREATE TABLE parent(x INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
        "CREATE TABLE child(y INTEGER PRIMARY KEY, pa INTEGER, pb INTEGER)",
    ])
    return path


@pytest.fixture(scope="module")
def attached_paths(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    d = tmp_path_factory.mktemp("duck_attached")
    main, other = d / "att_main.duckdb", d / "aaa.duckdb"
    _duck(main, ["CREATE TABLE t(id INTEGER PRIMARY KEY, u VARCHAR UNIQUE, v INTEGER)"])
    _duck(other, ["CREATE TABLE t(id INTEGER UNIQUE, u VARCHAR, v INTEGER PRIMARY KEY)"])
    return main, other


def _ds(path: Path, *, kind: str = "duckdb") -> DatasourceConfig:
    return DatasourceConfig(name="ds", type=kind, database=str(path))


def _models(report: IngestionScanReport) -> dict[str, SlayerModel]:
    return {m.name: m for m in report.models}


def _pk(model: SlayerModel) -> list[str]:
    return [c.name for c in model.columns if c.primary_key]


def _unique(model: SlayerModel) -> set[str]:
    return {c.name for c in model.columns if c.unique}


def _joins(model: SlayerModel) -> dict[str, tuple[list[list[str]], JoinCardinality | None]]:
    return {j.target_model: (j.join_pairs, j.cardinality) for j in model.joins}


def _raise_regclass(*_args: Any, **_kwargs: Any) -> NoReturn:
    raise exc.ProgrammingError("SELECT ...", {}, Exception(_REGCLASS))


@pytest.fixture
def broken_generic_reflection(monkeypatch: pytest.MonkeyPatch) -> None:
    """SQLAlchemy >= 2.0.55 on DuckDB: every generic key / column read raises."""
    for name in ("get_pk_constraint", "get_foreign_keys", "get_unique_constraints", "get_columns"):
        monkeypatch.setattr(Inspector, name, _raise_regclass)


# ---------------------------------------------------------------------------
# DuckDB: every declared key
# ---------------------------------------------------------------------------


class TestDuckdbKeys:
    def test_unique_constraints_mark_columns_unique(self, keys_db) -> None:
        customers = _models(ingest_datasource_report(datasource=_ds(keys_db)))["customers"]
        assert _pk(customers) == ["id"]
        assert _unique(customers) == {"code", "Mixed Case"}

    def test_fk_to_a_unique_column_is_to_one(self, keys_db) -> None:
        orders = _models(ingest_datasource_report(datasource=_ds(keys_db)))["orders"]
        pairs, cardinality = _joins(orders)["customers"]
        assert pairs == [["customer_code", "code"]]
        assert cardinality in _TO_ONE

    def test_composite_unique_is_a_key_for_join_cardinality(self, keys_db) -> None:
        pairs_model = _models(ingest_datasource_report(datasource=_ds(keys_db)))["pairs"]
        pairs, cardinality = _joins(pairs_model)["customers"]
        assert pairs == [["a", "a"], ["b", "b"]]
        assert cardinality in _TO_ONE

    def test_composite_pk(self, keys_db) -> None:
        lines = _models(ingest_datasource_report(datasource=_ds(keys_db)))["lines"]
        assert _pk(lines) == ["order_id", "n"]
        assert _joins(lines)["orders"][0] == [["order_id", "id"]]

    def test_expression_unique_index_is_no_key(self, keys_db) -> None:
        customers = _models(ingest_datasource_report(datasource=_ds(keys_db)))["customers"]
        assert "name" not in _unique(customers)

    def test_composite_fk_in_non_default_schema(self, schema_db) -> None:
        models = _models(ingest_datasource_report(datasource=_ds(schema_db), schemas=["ofr"]))
        assert _pk(models["parent"]) == ["a", "b"]
        assert _pk(models["child"]) == ["id"]
        pairs, cardinality = _joins(models["child"])["parent"]
        assert pairs == [["pa", "a"], ["pb", "b"]]
        assert cardinality in _TO_ONE

    def test_default_schema_twins_keep_their_own_keys(self, schema_db) -> None:
        models = _models(ingest_datasource_report(datasource=_ds(schema_db)))
        assert _pk(models["parent"]) == ["x"]
        assert models["child"].joins == []


@contextmanager
def _attached(paths: tuple[Path, Path]) -> Iterator[sa.Engine]:
    main, other = paths
    with disposable_engine(f"duckdb:///{main}") as engine:

        @event.listens_for(engine, "connect")
        def _attach(dbapi_conn, _rec):  # noqa: ANN001
            dbapi_conn.execute(f"ATTACH IF NOT EXISTS '{other}' AS aaa")

        yield engine


class TestAttachedCatalog:
    def test_attached_twin_contributes_no_keys(self, attached_paths) -> None:
        with _attached(attached_paths) as engine:
            model = introspect_table_to_model(
                sa_engine=engine, inspector=sa.inspect(engine),
                table_name="t", schema=None, data_source="ds",
            )
        assert _pk(model) == ["id"]
        assert _unique(model) == {"u"}


# ---------------------------------------------------------------------------
# DuckDB never goes through SQLAlchemy's generic (Postgres) catalog queries
# ---------------------------------------------------------------------------


class TestGenericReflectionBypassed:
    def test_every_key_join_and_column_survives(self, keys_db, broken_generic_reflection) -> None:
        report = ingest_datasource_report(datasource=_ds(keys_db))
        models = _models(report)
        assert report.skipped == []
        assert set(models) == {"customers", "orders", "pairs", "lines"}
        assert [c.name for c in models["customers"].columns] == [
            "id", "code", "a", "b", "Mixed Case", "name",
        ]
        assert _pk(models["customers"]) == ["id"]
        assert _unique(models["customers"]) == {"code", "Mixed Case"}
        assert _pk(models["lines"]) == ["order_id", "n"]
        assert _joins(models["orders"])["customers"][0] == [["customer_code", "code"]]
        assert _joins(models["pairs"])["customers"][0] == [["a", "a"], ["b", "b"]]
        assert _joins(models["lines"])["orders"][0] == [["order_id", "id"]]
        amount = next(c for c in models["orders"].columns if c.name == "amount")
        assert (amount.type, amount.db_type) == (DataType.DOUBLE, "DECIMAL(12,2)")

    def test_non_default_schema_join_survives(self, schema_db, broken_generic_reflection) -> None:
        models = _models(ingest_datasource_report(datasource=_ds(schema_db), schemas=["ofr"]))
        assert _joins(models["child"])["parent"][0] == [["pa", "a"], ["pb", "b"]]


# ---------------------------------------------------------------------------
# A failed key read: logged at WARNING, table still ingested
# ---------------------------------------------------------------------------


class _FailedRead(BaseModel):
    """One DuckDB key read made to fail: its catalog source, the statement marker, its log word."""

    table: str
    source: str
    marker: str
    word: str


_FAILED_READS = {
    "primary_key": _FailedRead(table="orders", source="duckdb_constraints", marker="primary key", word="primary"),
    "foreign_keys": _FailedRead(table="orders", source="duckdb_constraints", marker="foreign key", word="foreign"),
    "unique": _FailedRead(table="customers", source="duckdb_constraints", marker="unique", word="unique"),
    "unique_index": _FailedRead(table="customers", source="duckdb_indexes", marker="", word="index"),
}


@contextmanager
def _failing(read: _FailedRead) -> Iterator[None]:
    """Raise on DuckDB's own catalog read of one key kind for one table."""

    def _boom(_conn, _cursor, statement, *_rest) -> None:  # noqa: ANN001
        low = statement.lower()
        if read.source in low and f"'{read.table}'" in low and read.marker in low:
            raise RuntimeError("boom-key-read")

    event.listen(sa.engine.Engine, "before_cursor_execute", _boom)
    try:
        yield
    finally:
        event.remove(sa.engine.Engine, "before_cursor_execute", _boom)


def _warnings_naming(caplog: pytest.LogCaptureFixture, table: str) -> list[str]:
    return [
        r.getMessage() for r in caplog.records
        if r.levelno == logging.WARNING and table in r.getMessage()
    ]


def _assert_one_read_warning(messages: list[str], *, word: str) -> None:
    # A table re-read as a join target logs each failed read; the warnings stay identical.
    matching = {m for m in messages if "boom-key-read" in m}
    assert len(matching) == 1, messages
    assert re.search(word, next(iter(matching)), re.IGNORECASE), matching


class TestKeyReadFailureIsVisible:
    @pytest.mark.parametrize("kind", list(_FAILED_READS))
    def test_duckdb_key_read_failure(self, keys_db, kind: str, caplog: pytest.LogCaptureFixture) -> None:
        read = _FAILED_READS[kind]
        caplog.set_level(logging.WARNING)
        with _failing(read):
            report = ingest_datasource_report(datasource=_ds(keys_db))
        models = _models(report)
        assert report.skipped == []
        assert set(models) == {"customers", "orders", "pairs", "lines"}
        orders, customers = models["orders"], models["customers"]
        assert [c.name for c in orders.columns] == ["id", "customer_code", "amount"]
        assert _joins(models["lines"])["orders"][0] == [["order_id", "id"]]
        assert _pk(orders) == ([] if kind == "primary_key" else ["id"])
        assert ("customers" in _joins(orders)) is (kind != "foreign_keys")
        assert _pk(customers) == ["id"]
        assert ("code" in _unique(customers)) is (kind != "unique")
        assert ("Mixed Case" in _unique(customers)) is (kind != "unique_index")
        _assert_one_read_warning(_warnings_naming(caplog, read.table), word=read.word)

    def test_base_fk_read_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    ) -> None:
        db = tmp_path / "fk.db"
        with transaction(str(db)) as conn:
            conn.executescript(
                """
                CREATE TABLE customers (id INTEGER PRIMARY KEY);
                CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id));
                CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER REFERENCES orders(id));
                """
            )
        real = Inspector.get_foreign_keys

        def _flaky(self: Inspector, table_name: str, *args: Any, **kwargs: Any) -> Any:
            if table_name == "orders":
                raise RuntimeError("boom-key-read")
            return real(self, table_name, *args, **kwargs)

        monkeypatch.setattr(Inspector, "get_foreign_keys", _flaky)
        caplog.set_level(logging.WARNING)
        report = ingest_datasource_report(datasource=_ds(db, kind="sqlite"))
        models = _models(report)
        assert report.skipped == []
        assert models["orders"].joins == []
        assert _joins(models["items"])["orders"][0] == [["order_id", "id"]]
        _assert_one_read_warning(_warnings_naming(caplog, "orders"), word="foreign")


# ---------------------------------------------------------------------------
# Base behaviour on SQLite through ingestion
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_db(tmp_path: Path) -> Path:
    db = tmp_path / "base.db"
    with transaction(str(db)) as conn:
        conn.executescript(
            """
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY, email TEXT UNIQUE, a INTEGER, b INTEGER,
                code TEXT, flag INTEGER, UNIQUE (a, b)
            );
            CREATE UNIQUE INDEX ux_code ON customers (code);
            CREATE UNIQUE INDEX ux_partial ON customers (flag) WHERE flag IS NOT NULL;
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id),
                a INTEGER, b INTEGER, FOREIGN KEY (a, b) REFERENCES customers(a, b)
            );
            """
        )
    return db


@pytest.fixture
def statement_log() -> Iterator[list[str]]:
    seen: list[str] = []

    def _record(_conn, _cursor, statement, *_rest) -> None:  # noqa: ANN001
        seen.append(statement)

    event.listen(sa.engine.Engine, "before_cursor_execute", _record)
    try:
        yield seen
    finally:
        event.remove(sa.engine.Engine, "before_cursor_execute", _record)


class TestSqliteBaseBehaviour:
    def test_keys_unique_flags_and_joins(self, sqlite_db) -> None:
        models = _models(ingest_datasource_report(datasource=_ds(sqlite_db, kind="sqlite")))
        customers, orders = models["customers"], models["orders"]
        assert _pk(customers) == ["id"]
        assert _unique(customers) == {"email", "code"}
        joins = {tuple(map(tuple, j.join_pairs)): j.cardinality for j in orders.joins}
        assert joins == {
            (("customer_id", "id"),): JoinCardinality.MANY_TO_ONE,
            (("a", "a"), ("b", "b")): JoinCardinality.MANY_TO_ONE,
        }

    def test_pk_is_inspector_authoritative(self, sqlite_db, statement_log) -> None:
        ingest_datasource_report(datasource=_ds(sqlite_db, kind="sqlite"))
        assert statement_log
        assert not [s for s in statement_log if "information_schema" in s.lower()]
