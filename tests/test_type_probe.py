"""Type probes: one dialect bounding hook, a typed door that executes them verbatim, AST-built trial queries."""
# pyright: reportUnnecessaryTypeIgnoreComment=true

from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import sqlalchemy as sa
import sqlglot
from pydantic import ValidationError
from sqlalchemy import event
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.engine.query_engine import SlayerQueryEngine, _sql_client_cache_key
from slayer.sql.client import (
    SlayerSQLClient,
    TypeProbe,
    get_column_types_sync,
    render_type_probe,
    sql_model_trial_query,
)
from slayer.sql.dialects import _ALL_DIALECTS, DuckdbDialect, PostgresDialect, SqlDialect, SqliteDialect, TsqlDialect
from slayer.sql.render.parse import parse_query, parse_table
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import disposable_engine


def _inner() -> exp.Select:
    return exp.select("a").from_("t")


# ── row bound ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("dialect", _ALL_DIALECTS, ids=lambda d: type(d).__name__)
def test_type_probe_bounds_the_query_by_the_dialect_row_count(dialect: SqlDialect) -> None:
    probe = dialect.type_probe(_inner())
    rows = 1 if isinstance(dialect, SqliteDialect) else 0
    assert dialect.type_probe_rows == rows
    assert probe.args["limit"].expression == exp.Literal.number(rows)
    (source,) = [probe.args["from_"].this]
    assert isinstance(source, exp.Subquery)
    assert source.alias == "_types"
    assert source.this == _inner()
    assert probe.expressions == [exp.Star()]


@pytest.mark.parametrize(("dialect", "tail"), [
    (SqliteDialect(), ") AS _types LIMIT 1"),
    (PostgresDialect(), ") AS _types LIMIT 0"),
    (DuckdbDialect(), ") AS _types LIMIT 0"),
])
def test_type_probe_renders_limit(dialect: SqlDialect, tail: str) -> None:
    sql = render_type_probe(_inner(), dialect=dialect).sql
    assert sql.startswith("SELECT * FROM (SELECT a FROM t")
    assert sql.endswith(tail)


def test_tsql_type_probe_renders_top_without_limit() -> None:
    sql = render_type_probe(_inner(), dialect=TsqlDialect()).sql
    assert sql.startswith("SELECT TOP 0 * FROM (")
    assert sql.endswith(") AS _types")
    assert "LIMIT" not in sql


def test_type_probe_is_frozen() -> None:
    probe = render_type_probe(_inner(), dialect=PostgresDialect())
    with pytest.raises(ValidationError):
        probe.sql = "SELECT 1"  # pyright: ignore[reportAttributeAccessIssue]


# ── the typed door ────────────────────────────────────────────────────────

@pytest.fixture
def sqlite_engine(tmp_path: Path):
    with disposable_engine(f"sqlite:///{tmp_path / 'probe.db'}") as engine:
        with engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE t (a INTEGER, b TEXT)"))
            conn.execute(sa.text("INSERT INTO t VALUES (1, 'x')"))
            conn.commit()
        yield engine


def test_door_executes_the_probe_verbatim(sqlite_engine: sa.Engine) -> None:
    seen: list[str] = []
    event.listen(sqlite_engine, "before_cursor_execute", lambda _c, _cur, statement, *_a: seen.append(statement))
    probe = render_type_probe(exp.select("a", "b").from_("t"), dialect=SqliteDialect())
    assert get_column_types_sync(probe, engine=sqlite_engine, db_type="sqlite") == {"a": "number", "b": "string"}
    assert seen == [probe.sql]


def test_sync_door_refuses_a_string(sqlite_engine: sa.Engine) -> None:
    with pytest.raises(TypeError):
        get_column_types_sync("SELECT a FROM t", engine=sqlite_engine, db_type="sqlite")  # pyright: ignore[reportArgumentType]


async def test_client_door_refuses_a_string() -> None:
    client = SlayerSQLClient(datasource=DatasourceConfig(name="mem", type="sqlite", database=":memory:"))
    try:
        with pytest.raises(TypeError):
            await client.get_column_types("SELECT 1")  # pyright: ignore[reportArgumentType]
    finally:
        client.close()


# ── trial query ───────────────────────────────────────────────────────────

def _trial(sql: str) -> exp.Select:
    return sql_model_trial_query(sql, dialect=PostgresDialect())


@pytest.mark.parametrize("sql", ["SELECT a FROM t", "SELECT a FROM t;", "SELECT a FROM t ;  \n",
                                 "SELECT a FROM t -- note"])
def test_trial_query_wraps_the_parsed_model_sql(sql: str) -> None:
    trial = _trial(sql)
    source = trial.args["from_"].this
    assert isinstance(source, exp.Subquery)
    assert source.alias == "_sd_validate"
    assert source.this.sql(dialect="postgres") == "SELECT a FROM t"
    assert trial.args["where"].this.sql() == "1 = 0"


@pytest.mark.parametrize("sql", ["DELETE FROM t", "SELECT a INTO u FROM t",
                                 "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x", "SELECT ((( not valid"])
def test_trial_query_refuses_what_is_not_a_read_only_query(sql: str) -> None:
    with pytest.raises(Exception):  # noqa: B017 — parse or read-only refusal
        _trial(sql)


# ── parse door ────────────────────────────────────────────────────────────

@pytest.mark.parametrize(("name", "parts", "rendered"), [
    ("order", ("", "", "order"), '"order"'),
    ('"Sales"."Order Items"', ("", "Sales", "Order Items"), '"Sales"."Order Items"'),
    ("Sales.OrderItems", ("", "Sales", "OrderItems"), '"Sales"."OrderItems"'),
    ("analytics.sales.orders", ("analytics", "sales", "orders"), "analytics.sales.orders"),
    ('"sales.v2".orders', ("", "sales.v2", "orders"), '"sales.v2".orders'),
])
def test_parse_table_reads_the_dialect_identifier_rules(name: str, parts: tuple[str, str, str],
                                                        rendered: str) -> None:
    table = parse_table(name=name, dialect=PostgresDialect())
    assert (table.catalog, table.db, table.name) == parts
    assert table.sql(dialect="postgres") == rendered


def test_parse_query_returns_the_read_only_query() -> None:
    tree = parse_query(sql='SELECT qty FROM "Order Items"', target_dialect=DuckdbDialect(),
                       parse_dialect=DuckdbDialect())
    assert isinstance(tree, exp.Select)
    assert tree.sql(dialect="duckdb") == 'SELECT qty FROM "Order Items"'


@pytest.mark.parametrize("sql", ["DELETE FROM t", "SELECT a INTO u FROM t", "SELECT a FROM t FOR UPDATE",
                                 "DROP TABLE t", "SELECT ((( not valid"])
def test_parse_query_refuses_what_is_not_a_read_only_query(sql: str) -> None:
    postgres = PostgresDialect()
    with pytest.raises(Exception):  # noqa: B017 — parse or read-only refusal
        parse_query(sql=sql, target_dialect=postgres, parse_dialect=postgres)


# ── engine model typing ───────────────────────────────────────────────────

def _orders(sql_table: str = "orders") -> SlayerModel:
    return SlayerModel(name="orders", sql_table=sql_table, data_source="test", columns=[
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="revenue", sql="amount", type=DataType.DOUBLE),
    ])


async def _captured_engine_probe(ds_type: str) -> str:
    storage = YAMLStorage(base_dir=tempfile.mkdtemp())
    await storage.save_datasource(DatasourceConfig(name="test", type="postgres"))
    seen: list[TypeProbe] = []

    async def capture(probe: TypeProbe) -> dict[str, str]:
        seen.append(probe)
        return {}

    with patch.object(storage, "get_model", new_callable=AsyncMock, return_value=_orders()):
        engine = SlayerQueryEngine(storage=storage)
        ds = MagicMock()
        ds.get_connection_string.return_value = "sqlite://"
        ds.type = ds_type
        ds.credentials_json = None
        with patch.object(engine, "_resolve_datasource", new_callable=AsyncMock, return_value=ds):
            client = MagicMock()
            client.get_column_types = capture
            engine._sql_clients[_sql_client_cache_key(ds)] = client
            await engine.get_column_types("orders")
    (probe,) = seen
    assert isinstance(probe, TypeProbe)
    return probe.sql


async def test_engine_probe_keeps_the_limit_wrapper() -> None:
    sql = await _captured_engine_probe("sqlite")
    assert sql.startswith("SELECT * FROM (")
    assert sql.endswith(") AS _types LIMIT 1")
    assert isinstance(sqlglot.parse_one(sql, dialect="sqlite").args["from_"].this, exp.Subquery)


async def test_engine_probe_keeps_the_top_wrapper_on_tsql() -> None:
    sql = await _captured_engine_probe("mssql")
    assert sql.startswith("SELECT TOP 0 * FROM (")
    assert sql.endswith(") AS _types")
    assert "LIMIT" not in sql


async def test_engine_probe_applies_the_session_policy(tmp_path: Path) -> None:
    db = str(tmp_path / "live.db")
    with transaction(db) as conn:
        conn.executescript("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, org TEXT);"
                           "INSERT INTO orders VALUES (1, 2.5, 'secret-org');")
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=db))
    await storage.save_model(_orders())
    engine = SlayerQueryEngine(storage=storage, policy=SessionPolicy(
        ruleset=ColumnFilterRuleset(column="org", value="secret-org")))
    seen: list[str] = []
    real = SlayerSQLClient.get_column_types

    async def spy(self: SlayerSQLClient, probe: TypeProbe) -> dict[str, str]:
        seen.append(probe.sql)
        return await real(self, probe)

    try:
        with patch.object(SlayerSQLClient, "get_column_types", spy):
            types = await engine.get_column_types("orders")
    finally:
        engine.close()
    assert types.get("revenue") == "number"
    (sql,) = seen
    assert "secret-org" in sql
    assert sql.endswith(") AS _types LIMIT 1")
