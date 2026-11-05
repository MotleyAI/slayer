"""How a stored ``sql_table`` names its table, and SLayer's reading of an identifier (spec: models/table-names;
importers/meaning-preservation › Identifier matching follows the source dialect)."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Sequence
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
import sqlglot
from sqlalchemy.sql.elements import quoted_name
from sqlglot import exp

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import (
    IntrospectedColumn, _columns_to_model, ingest_datasource_idempotent, list_ingestable_objects,
)
from slayer.engine.introspect_utils import table_location
from slayer.engine import schema_drift
from slayer.engine.schema_drift import DriftSnapshot, LiveTable, _list_live_objects
from slayer.engine.schema_scope import SchemaRef, is_quoted_name
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.importers.physical import PhysicalIndex
from slayer.sql import engine_factory
from slayer.sql.client import DelegatingSqlClient, ObjectListing, SlayerSQLClient
from slayer.sql.dialects import get_dialect
from slayer.sql.naming import denoted_name, render_table_name
from slayer.sql.render.parse import parse_table
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import _engine_generate, disposable_engine

DS = "ds"


# ── SLayer's reading of an identifier ─────────────────────────────────────

@pytest.mark.parametrize(("dialect", "sql", "expected"), [
    ("postgres", "foo", "foo"),
    ("postgres", "Foo", "Foo"),
    ("postgres", "FOO", "FOO"),
    ("postgres", '"Foo"', "Foo"),
    ("redshift", "foo", "foo"),
    ("snowflake", "foo", "FOO"),
    ("snowflake", "Foo", "Foo"),
    ("snowflake", '"foo"', "foo"),
    ("oracle", "foo", "FOO"),
    ("oracle", '"foo"', "foo"),
    ("duckdb", "Foo", "Foo"),
    ("mysql", "Foo", "Foo"),
    ("clickhouse", "Foo", "Foo"),
])
def test_denoted_name(dialect: str, sql: str, expected: str) -> None:
    column = sqlglot.parse_one(sql, read=dialect)
    assert isinstance(column, exp.Column)
    assert denoted_name(column.this, dialect=dialect) == expected


@pytest.mark.parametrize("dialect", ["snowflake", "oracle"])
def test_folded_and_exact_live_spellings_are_ambiguous(dialect: str) -> None:
    """SQLAlchemy reflects ``FOO`` as ``foo``, ``"foo"`` as a quoted ``foo`` and ``"Foo"`` as ``Foo``."""
    index = PhysicalIndex([], dialect=dialect, live=["foo", quoted_name("foo", True), "Foo"])
    problem = index.problem(exp.to_identifier("foo"))
    assert problem is not None
    assert "ambiguous" in problem
    assert index.problem(exp.to_identifier("Foo", quoted=True)) is None
    assert index.problem(exp.to_identifier("foo", quoted=True)) is None


def test_unquoted_mixed_case_reference_reads_the_exact_column_on_postgres() -> None:
    lower, exact = Column(name="lower", sql="orderamount"), Column(name="exact", sql='"OrderAmount"')
    index = PhysicalIndex([lower, exact], dialect="postgres", live=["orderamount", "OrderAmount"])
    assert index.reader(exp.to_identifier("OrderAmount")) is exact
    assert index.reader(exp.to_identifier("orderamount")) is lower


def test_case_insensitive_dialect_matches_any_case() -> None:
    col = Column(name="amount", sql="orderamount")
    index = PhysicalIndex([col], dialect="duckdb", live=["orderamount"])
    assert index.reader(exp.to_identifier("OrderAmount")) is col


# ── the one writer and the one reader ─────────────────────────────────────

@pytest.mark.parametrize(("parts", "expected"), [
    (("sales", "order items"), 'sales."order items"'),
    (("sales.v2", "orders"), '"sales.v2".orders'),
    (("sales", 'we"ird'), 'sales."we""ird"'),
    (("analytics", "sales", "orders"), "analytics.sales.orders"),
    (("order",), '"order"'),
    (("2026_orders",), '"2026_orders"'),
    (("OrderItems",), "OrderItems"),
    (("Sales", "OrderItems"), "Sales.OrderItems"),
])
def test_render_table_name(parts: tuple[str, ...], expected: str) -> None:
    assert render_table_name(parts=parts, dialect=get_dialect("postgres")) == expected


@pytest.mark.parametrize("dialect", ["snowflake", "oracle"])
def test_render_table_name_quotes_a_reflected_lowercase_name(dialect: str) -> None:
    """SQLAlchemy reflects ``"lower"`` on an upper-folding database as a quoted ``lower``."""
    parts = [quoted_name("sales", True), quoted_name("orders", True)]
    assert render_table_name(parts=parts, dialect=get_dialect(dialect)) == '"sales"."orders"'


def _parts(name: str, *, dialect: str) -> list[str]:
    table = parse_table(name=name, dialect=get_dialect(dialect))
    return [p for p in (table.catalog, table.db, table.name) if p]


@pytest.mark.parametrize("dialect", ["postgres", "snowflake", "mysql", "duckdb", "sqlite", "tsql"])
@pytest.mark.parametrize("parts", [
    ("sales", "order items"), ("sales.v2", "orders"), ("sales", 'we"ird'), ("sales", "we`ird"),
    ("analytics", "sales", "orders"), ("order",), ("2026_orders",), ("OrderItems",), ("Sales", "OrderItems"),
])
def test_rendered_name_reads_back_as_its_parts(parts: Sequence[str], dialect: str) -> None:
    rendered = render_table_name(parts=parts, dialect=get_dialect(dialect))
    assert _parts(rendered, dialect=dialect) == list(parts)


# ── ingestion stores names that read back (in-process backends) ──────────

async def _query(storage: YAMLStorage, model: str) -> list[dict[str, object]]:
    engine = SlayerQueryEngine(storage=storage)
    try:
        return (await engine.execute({"source_model": model, "dimensions": ["qty"]})).data
    finally:
        engine.close()


async def _validate(storage: YAMLStorage) -> list[object]:
    engine = SlayerQueryEngine(storage=storage)
    try:
        return await engine.validate_models(data_source=DS)
    finally:
        engine.close()


def _seed(url: str, statements: Sequence[str]) -> None:
    with disposable_engine(url) as engine, engine.connect() as conn:
        for statement in statements:
            conn.execute(sa.text(statement))
        conn.commit()


@pytest.fixture
async def duck(tmp_path: Path) -> AsyncGenerator[tuple[DatasourceConfig, YAMLStorage]]:
    db = tmp_path / "shop.duckdb"
    _seed(f"duckdb:///{db}", ("CREATE SCHEMA sales", 'CREATE TABLE sales."order items" (qty INTEGER)',
                              'INSERT INTO sales."order items" VALUES (6)'))
    ds = DatasourceConfig(name=DS, type="duckdb", database=str(db))
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(ds)
    try:
        yield ds, storage
    finally:
        engine_factory.invalidate_engine(ds)


async def _only_model(storage: YAMLStorage) -> SlayerModel:
    (name,) = await storage.list_models()
    model = await storage.get_model(name)
    assert model is not None
    return model


@pytest.mark.parametrize("scope", [{"schema": "sales"}, {"all_schemas": True}])
async def test_ingested_name_with_a_space_reads_back(duck: tuple[DatasourceConfig, YAMLStorage],
                                                     scope: dict[str, Any]) -> None:
    ds, storage = duck
    await ingest_datasource_idempotent(datasource=ds, storage=storage, **scope)
    model = await _only_model(storage)
    assert model.sql_table == 'sales."order items"'
    assert [r[f"{model.name}.qty"] for r in await _query(storage, model.name)] == [6]
    again = await ingest_datasource_idempotent(datasource=ds, storage=storage, **scope)
    assert [(a.created, a.new_columns) for a in again.additions] == [(False, [])]
    assert again.to_delete == []
    assert await _validate(storage) == []


async def test_stored_dotted_table_name_validates_on_sqlite(tmp_path: Path) -> None:
    db = tmp_path / "shop.db"
    _seed(f"sqlite:///{db}", ('CREATE TABLE "order.items" (qty INTEGER)', 'INSERT INTO "order.items" VALUES (1)'))
    ds = DatasourceConfig(name=DS, type="sqlite", database=str(db))
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(ds)
    try:
        await storage.save_model(SlayerModel(name="items", data_source=DS, sql_table='"order.items"',
                                             columns=[Column(name="qty", type=DataType.INT)]))
        assert [r["items.qty"] for r in await _query(storage, "items")] == [1]
        assert await _validate(storage) == []
    finally:
        engine_factory.invalidate_engine(ds)


class _ReflectingClient(DelegatingSqlClient):
    """Test double: a client listing ``"lower_items"`` as SQLAlchemy reflects it on Snowflake."""

    async def list_objects(self, schema: str | None) -> ObjectListing:
        return ObjectListing(tables=[quoted_name("lower_items", True), "orders"])


async def test_listed_objects_keep_their_quoting() -> None:
    datasource = DatasourceConfig(name=DS, type="snowflake")
    client = _ReflectingClient(inner=SlayerSQLClient(datasource=datasource))
    objects = await list_ingestable_objects(client=client, ref=SchemaRef(name="s"))
    assert [(o.name, o.quoted) for o in objects] == [("lower_items", True), ("orders", False)]
    snowflake = get_dialect("snowflake")
    assert [SchemaRef(name="s", requested="s").qualify(o.live_name, dialect=snowflake) for o in objects] == [
        's."lower_items"', "s.orders"]
    ((_, names),) = await _list_live_objects([SchemaRef(name="s")], client=client, datasource=datasource)
    assert [is_quoted_name(n) for n in names] == [True, False]


class _TwinsClient(DelegatingSqlClient):
    """Test double: Snowflake's table ``FOO`` and view ``"foo"``, both reported ``foo``."""

    async def list_objects(self, schema: str | None) -> ObjectListing:
        return ObjectListing(tables=["foo"], views=[quoted_name("foo", True)])


async def test_listed_folded_and_exact_twins_stay_two_objects() -> None:
    client = _TwinsClient(inner=SlayerSQLClient(datasource=DatasourceConfig(name=DS, type="snowflake")))
    objects = await list_ingestable_objects(client=client, ref=SchemaRef(name="s"))
    assert [(o.name, o.quoted, o.kind) for o in objects] == [("foo", False, "table"), ("foo", True, "view")]


async def test_drift_snapshot_reads_folded_and_exact_twins_separately(monkeypatch: pytest.MonkeyPatch) -> None:
    read: list[bool] = []

    async def introspect(*, obj_name: str, **_kwargs: Any) -> LiveTable:
        read.append(is_quoted_name(obj_name))
        return LiveTable(columns={})

    monkeypatch.setattr(schema_drift, "_introspect_live_object", introspect)
    snapshot, ref = DriftSnapshot(created_at=0.0), SchemaRef(name="s")
    datasource = DatasourceConfig(name=DS, type="snowflake")
    client = _TwinsClient(inner=SlayerSQLClient(datasource=datasource))
    for name in ("foo", quoted_name("foo", True), "foo"):
        await snapshot._table(client=client, datasource=datasource, ref=ref, obj_name=name)
    assert read == [False, True]


def test_table_location_keeps_the_quoting_of_its_names() -> None:
    location = table_location(quoted_name("lower_items", True), SchemaRef(name=quoted_name("lower_schema", True)))
    assert [is_quoted_name(n) for n in (location.name, location.schema_name)] == [True, True]


# ── a quoted physical key reads its exact-case column ─────────────────────

def _keyed(*, cardinality: JoinCardinality) -> list[SlayerModel]:
    customers = SlayerModel(name="customers", data_source=DS, sql_table="customers", columns=[
        Column(name="id", sql='"id"', type=DataType.INT, primary_key=True), Column(name="name", type=DataType.TEXT)],
        joins=[ModelJoin(target_model="orders", join_pairs=[["id", "cust"]], cardinality=cardinality)])
    orders = SlayerModel(name="orders", data_source=DS, sql_table="orders", columns=[
        Column(name="oid", type=DataType.INT, primary_key=True), Column(name="cust", sql='"cust_key"', type=DataType.INT),
        Column(name="status", type=DataType.TEXT)])
    return [customers, orders]


def _key_columns(sql: str, *, dialect: str) -> set[tuple[str, bool]]:
    tree = sqlglot.parse_one(sql, read=dialect)
    keys = {"id", "cust_key"}
    return {(c.name, c.this.quoted) for c in tree.find_all(exp.Column) if c.name in keys}


@pytest.mark.parametrize(("cardinality", "query"), [
    (JoinCardinality.ONE_TO_ONE, {"source_model": "customers", "dimensions": ["name", "orders.status"]}),
    (JoinCardinality.ONE_TO_MANY, {"source_model": "customers", "dimensions": ["name"],
                                   "filters": ["orders.status = 'x'"]}),
    (JoinCardinality.ONE_TO_MANY, {"source_model": "customers", "dimensions": ["name"],
                                   "filters": ["name = 'a' OR orders.status = 'x'"]}),
])
@pytest.mark.parametrize("dialect", ["snowflake", "postgres"])
async def test_quoted_join_keys_stay_quoted(cardinality: JoinCardinality, query: dict[str, Any], dialect: str) -> None:
    customers, orders = _keyed(cardinality=cardinality)
    sql = await _engine_generate(query=SlayerQuery.model_validate(query), model=customers, extra_models=[orders],
                                 dialect=dialect)
    assert _key_columns(sql, dialect=dialect) == {("id", True), ("cust_key", True)}


def test_ingested_quoted_column_reads_itself_exactly() -> None:
    model = _columns_to_model(name="t", data_source=DS, sql_table="t", columns=[
        IntrospectedColumn(name="foo", type=DataType.INT, quoted=True), IntrospectedColumn(name="bar", type=DataType.INT)])
    assert [(c.name, c.sql, c.physical_name, c.physical_quoted) for c in model.columns] == [
        ("foo", '"foo"', "foo", True), ("bar", "bar", "bar", False)]
