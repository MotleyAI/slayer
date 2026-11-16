"""Associated homes are deduplicated by their primary key, else by a per-query row identity
(spec: queries/attribution-modes › Per-query row identity for keyless homes)."""

from __future__ import annotations

import re

import pytest
from pydantic import ValidationError
from pytest_postgresql import factories

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.sql import naming
from tests._dev1841_fixtures import keyless_root_models
from tests._dev1841_fixtures import make_exec_engine as dev1841_engine
from tests._dev2216_fixtures import (
    DUP_COUNT_BY_STATUS,
    FILTERED_SPEND_BY_STATUS,
    SPEND_BY_STATUS,
    WAVG_BY_STATUS,
    WSUM_BY_STATUS,
    assoc,
    cells,
    kinds,
    measure,
    ossie_engine,
    ossie_models,
    query,
    seed_with,
)
from tests._engine_helpers import _engine_generate, seeded_exec_engine

postgresql_proc = factories.postgresql_proc(port=None)

CUSTOMER_MEASURES = [
    measure("sum(customers.spend)", "sp"),
    measure("wsum(customers.spend)", "ws"),
    measure("weighted_avg(customers.spend, weight=customers.spend)", "wa"),
]
COUNT_CUSTOMERS = [measure("count(customers.*)", "n")]
#: Dialects whose ``ROW_NUMBER()`` rejects an empty window.
ORDERED_ROW_NUMBER = {"tsql", "snowflake", "databricks", "spark", "oracle"}
ALL_DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake",
                "clickhouse", "trino", "redshift", "databricks", "spark", "oracle"]
ROW_NUMBER = re.compile(r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(([^)]*)\)", re.IGNORECASE)

#: #597's sales/tags data: two sales share tag 1 and a NULL (or distinct) ``source_ref``; tag 1 is stored twice.
SALES_BY_LABEL = {"shared": 80.0, "other": 90.0}
SALES_COUNT_BY_LABEL = {"shared": 3.0, "other": 1.0}
SALES_WAVG_BY_LABEL = {"shared": 30.0, "other": 90.0}


@pytest.fixture(params=["sqlite", "duckdb"])
def dialect(request) -> str:
    return request.param


def sales_statements(*, nullable: bool, keyed: bool = False) -> list[str]:
    b, c = ("NULL", "NULL") if nullable else ("'b'", "'c'")
    rows = [("'a'", 1, 10, 1), (b, 1, 30, 1), (c, 1, 40, 2), ("'d'", 2, 90, 1)]
    values = ", ".join(
        "(" + ", ".join(str(v) for v in ((i,) if keyed else ()) + row) + ")" for i, row in enumerate(rows, start=1)
    )
    pk = "id INTEGER PRIMARY KEY, " if keyed else ""
    return [
        f"CREATE TABLE sales ({pk}source_ref VARCHAR UNIQUE, tag_id INTEGER, amount INTEGER, weight INTEGER)",
        "CREATE TABLE tags (tag_id INTEGER, label VARCHAR)",
        f"INSERT INTO sales VALUES {values}",
        "INSERT INTO tags VALUES (1, 'shared'), (1, 'shared'), (2, 'other')",
    ]


def sales_measures(prefix: str) -> list:
    return [
        measure(f"sum({prefix}amount)", "total"),
        measure("count(sales.*)", "n"),
        measure(f"weighted_avg({prefix}amount, weight={prefix}weight)", "weighted"),
    ]


def assert_sales_cells(resp) -> None:
    assert cells(resp, "label", "total") == SALES_BY_LABEL
    assert cells(resp, "label", "n") == SALES_COUNT_BY_LABEL
    assert cells(resp, "label", "weighted") == pytest.approx(SALES_WAVG_BY_LABEL)
    assert kinds(resp, "associated")
    assert not [k for row in resp.data for k in row if "__slayer_" in k]


def keyed_sales_models() -> list[SlayerModel]:
    """``sales`` with a primary key and a nullable standalone unique ``source_ref``."""
    sales = SlayerModel(
        name="sales", data_source="test", sql_table="sales",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="source_ref", type=DataType.TEXT, unique=True),
            Column(name="tag_id", type=DataType.INT),
            Column(name="amount", type=DataType.INT),
            Column(name="weight", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="tags", join_pairs=[["tag_id", "tag_id"]])],
    )
    tags = SlayerModel(
        name="tags", data_source="test", sql_table="tags",
        columns=[Column(name="tag_id", type=DataType.INT), Column(name="label", type=DataType.TEXT)],
    )
    return [sales, tags]


async def count_sql(*, dialect: str, keyed: bool = False, unique: bool = False) -> str:
    models = ossie_models(keyed=keyed, unique=unique)
    return await _engine_generate(
        query=assoc(source_model="orders", dimensions=["status"], measures=COUNT_CUSTOMERS),
        model=models[0], extra_models=models[1:], dialect=dialect, validate=False,
    )


class TestKeylessHome:
    async def test_matches_the_keyed_model(self, dialect) -> None:
        q = assoc(source_model="orders", dimensions=["status"], measures=CUSTOMER_MEASURES)
        async with ossie_engine(dialect, models=ossie_models(keyed=True)) as engine:
            keyed = await engine.execute(q)
        async with ossie_engine(dialect) as engine:
            keyless = await engine.execute(q)
        assert cells(keyless, "status", "sp") == cells(keyed, "status", "sp")
        assert cells(keyless, "status", "ws") == cells(keyed, "status", "ws")
        assert cells(keyless, "status", "sp") == SPEND_BY_STATUS
        assert cells(keyless, "status", "ws") == WSUM_BY_STATUS
        assert cells(keyless, "status", "wa") == pytest.approx(WAVG_BY_STATUS)
        assert kinds(keyless, "associated")


class TestRowIdentity:
    @pytest.mark.parametrize("nullable", [True, False], ids=["nullable-unique", "all-non-null"])
    @pytest.mark.parametrize("source", ["sales", "tags"], ids=["local", "cross-model"])
    async def test_nullable_unique_keeps_each_sale(self, dialect, nullable, source) -> None:
        stmts = sales_statements(nullable=nullable)
        async with seeded_exec_engine(dialect=dialect, seed=seed_with(dialect, stmts), models=[]) as (engine, _):
            ds = await engine.storage.get_datasource("test")
            assert ds is not None
            models = await engine.ingest(ds)
            sales = next(model for model in models if model.name == "sales")
            assert next(col for col in sales.columns if col.name == "source_ref").unique
            assert not any(col.primary_key for col in sales.columns)
            sales.joins = [ModelJoin(target_model="tags", join_pairs=[["tag_id", "tag_id"]])]
            for model in models:
                await engine.storage.save_model(model, _validate=False)
            dimension = "tags.label" if source == "sales" else "label"
            prefix = "" if source == "sales" else "sales."
            resp = await engine.execute(assoc(source_model=source, dimensions=[dimension],
                                              measures=sales_measures(prefix)))
        assert_sales_cells(resp)

    async def test_duplicate_rows_count_separately_fan_out_copies_once(self, dialect) -> None:
        async with ossie_engine(dialect, duplicate=True) as engine:
            resp = await engine.execute(assoc(source_model="orders", dimensions=["status"],
                                              measures=COUNT_CUSTOMERS))
        assert cells(resp, "status", "n") == DUP_COUNT_BY_STATUS
        assert not [k for row in resp.data for k in row if "__slayer_" in k]

    async def test_fan_out_under_a_joined_filter_counts_each_row_once(self, dialect) -> None:
        async with ossie_engine(dialect) as engine:
            resp = await engine.execute(assoc(
                source_model="customers", dimensions=["orders.status"], measures=[measure("sum(spend)", "sp")],
                filters=["orders.amount >= 50"]))
        assert cells(resp, "status", "sp") == FILTERED_SPEND_BY_STATUS

    async def test_filtered_keyless_home_numbers_its_filtered_rows(self, dialect) -> None:
        models = ossie_models()
        models[1].filters = ["spend >= 20"]
        async with ossie_engine(dialect, models=models) as engine:
            resp = await engine.execute(assoc(
                source_model="orders", dimensions=["status"], measures=[measure("sum(customers.spend)", "sp")]))
        assert cells(resp, "status", "sp") == {"completed": 30.0, "pending": 20.0}

    async def test_a_regroup_prefixed_home_column_does_not_shadow_the_row_identity(self, dialect) -> None:
        models = ossie_models()
        models[1].columns = [*models[1].columns, Column(name="__regroup__row_id", sql="id", type=DataType.INT)]
        async with ossie_engine(dialect, models=models) as engine:
            resp = await engine.execute(assoc(
                source_model="orders", dimensions=["status"], measures=[measure("sum(customers.spend)", "sp")]))
        assert cells(resp, "status", "sp") == SPEND_BY_STATUS

    def test_no_model_column_can_take_the_row_identity_name(self) -> None:
        with pytest.raises(ValidationError):
            Column(name=naming.ROW_IDENTITY_COLUMN, type=DataType.INT)

    async def test_postgres_executes(self, request) -> None:
        async with ossie_engine("postgres", request=request, duplicate=True) as engine:
            resp = await engine.execute(assoc(source_model="orders", dimensions=["status"],
                                              measures=COUNT_CUSTOMERS))
        assert cells(resp, "status", "n") == DUP_COUNT_BY_STATUS

    @pytest.mark.parametrize("target", ALL_DIALECTS)
    async def test_every_dialect_numbers_the_home_rows(self, target) -> None:
        sql = await count_sql(dialect=target)
        over = ROW_NUMBER.findall(sql)
        assert over, sql
        if target in ORDERED_ROW_NUMBER:
            assert all("ORDER BY" in clause.upper() for clause in over), sql

    async def test_keyed_homes_are_not_numbered(self) -> None:
        sql = await count_sql(dialect="duckdb", keyed=True)
        assert not ROW_NUMBER.search(sql), sql

    async def test_unique_only_homes_are_numbered(self) -> None:
        sql = await count_sql(dialect="duckdb", unique=True)
        assert ROW_NUMBER.search(sql), sql


class TestPrimaryKeyWithNullableUnique:
    """A primary key, not a nullable unique column, deduplicates the home."""

    async def test_sql_is_not_numbered(self) -> None:
        sales, tags = keyed_sales_models()
        sql = await _engine_generate(
            query=assoc(source_model="sales", dimensions=["tags.label"], measures=sales_measures("")),
            model=sales, extra_models=[tags], dialect="duckdb", validate=False,
        )
        assert not ROW_NUMBER.search(sql), sql

    @pytest.mark.parametrize("source", ["sales", "tags"], ids=["local", "cross-model"])
    async def test_null_unique_rows_contribute(self, dialect, source) -> None:
        stmts = sales_statements(nullable=True, keyed=True)
        async with seeded_exec_engine(
            dialect=dialect, seed=seed_with(dialect, stmts), models=keyed_sales_models(),
        ) as (engine, _):
            dimension = "tags.label" if source == "sales" else "label"
            prefix = "" if source == "sales" else "sales."
            resp = await engine.execute(assoc(source_model=source, dimensions=[dimension],
                                              measures=sales_measures(prefix)))
        assert_sales_cells(resp)


class TestAttributableKeylessHome:
    """No dimension needs association, so a keyless home takes the plain path."""

    @pytest.fixture(params=["sqlite", "duckdb"])
    async def engine(self, request):
        models = keyless_root_models()
        models[0].joins = [j.model_copy(update={"cardinality": JoinCardinality.MANY_TO_ONE})
                           if j.target_model == "customers" else j for j in models[0].joins]
        async for engine in dev1841_engine(request, models=models):
            yield engine

    async def test_same_values_as_broadcast_without_association(self, engine) -> None:
        wavg = [measure("weighted_avg(customers.spend, weight=sum(amount, partition_by=customers.regions.name))",
                        "wv")]
        kw = {"source_model": "orders", "dimensions": ["customers.tier"], "measures": wavg}
        associated = await engine.execute(assoc(**kw))
        broadcast = await engine.execute(query(**kw, to_many_handling="broadcast"))
        assert cells(associated, "tier", "wv") == cells(broadcast, "tier", "wv")
        assert not kinds(associated, "associated")
        assert not kinds(associated, "broadcast")
