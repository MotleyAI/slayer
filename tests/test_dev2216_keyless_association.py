"""Keyless homes associate through a per-query row identity; unproven hops associate by default
(spec: queries/attribution-modes › Per-query row identity for keyless homes, Mode selection and the
association default)."""

from __future__ import annotations

import re

import pytest
from pydantic import ValidationError
from pytest_postgresql import factories

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import SlayerError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.sql.naming import ROW_IDENTITY_FIELD
from tests._dev1841_fixtures import keyless_root_datasets
from tests._dev1841_fixtures import make_exec_engine as dev1841_engine
from tests._dev2216_fixtures import (
    AMOUNT_BY_REGION,
    AMOUNT_BY_REGION_ROOTED,
    AMOUNT_TOTAL,
    DUP_COUNT_BY_STATUS,
    FILTERED_SPEND_BY_STATUS,
    SPEND_BY_STATUS,
    WAVG_BY_STATUS,
    WSUM_BY_STATUS,
    cells,
    kinds,
    metric,
    ossie_engine,
    ossie_datasets,
    query,
    seed_with,
)
from tests._engine_helpers import _engine_generate, seeded_exec_engine

postgresql_proc = factories.postgresql_proc(port=None)

MODES = [pytest.param({}, id="default"), pytest.param({"to_many_handling": "associate"}, id="associate")]
REV_ROOTLESS = [metric("sum(orders.amount)", "rev")]
REV_ROOTED = [metric("sum(amount)", "rev")]
CUSTOMER_MEASURES = [
    metric("sum(customers.spend)", "sp"),
    metric("wsum(customers.spend)", "ws"),
    metric("weighted_avg(customers.spend, weight=customers.spend)", "wa"),
]
#: Dialects whose ``ROW_NUMBER()`` rejects an empty window.
ORDERED_ROW_NUMBER = {"tsql", "snowflake", "databricks", "spark", "oracle"}
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


def sales_query(*, source: str, mode: dict):
    dimension = "tags.label" if source == "sales" else "label"
    prefix = "" if source == "sales" else "sales."
    return query(
        population=source, dimensions=[dimension],
        metrics=[
            metric(f"sum({prefix}amount)", "total"),
            metric("count(sales.*)", "n"),
            metric(f"weighted_avg({prefix}amount, weight={prefix}weight)", "weighted"),
        ],
        **mode,
    )


def assert_sales_cells(resp) -> None:
    assert cells(resp, "label", "total") == SALES_BY_LABEL
    assert cells(resp, "label", "n") == SALES_COUNT_BY_LABEL
    assert cells(resp, "label", "weighted") == pytest.approx(SALES_WAVG_BY_LABEL)
    assert kinds(resp, "associated")
    assert not [k for row in resp.data for k in row if "__slayer_" in k]


def keyed_sales_datasets() -> list[SlayerDataset]:
    """``sales`` with a primary key and a nullable standalone unique ``source_ref``."""
    sales = SlayerDataset(
        name="sales", data_source="test", sql_table="sales",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="source_ref", datatype=DataType.STRING, unique=True),
            DatasetField(name="tag_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.INTEGER),
            DatasetField(name="weight", datatype=DataType.INTEGER),
        ],
        relationships=[DatasetRelationship(to="tags", from_columns=["tag_id"], to_columns=["tag_id"])],
    )
    tags = SlayerDataset(
        name="tags", data_source="test", sql_table="tags",
        fields=[DatasetField(name="tag_id", datatype=DataType.INTEGER), DatasetField(name="label", datatype=DataType.STRING)],
    )
    return [sales, tags]


async def count_sql(*, dialect: str, keyed: bool = False, unique: bool = False) -> str:
    models = ossie_datasets(keyed=keyed, unique=unique)
    return await _engine_generate(
        query=query(population="orders", dimensions=["orders.status"], metrics=[metric("count(customers.*)", "n")]),
        dataset=models[0], extra_datasets=models[1:], dialect=dialect, validate=False,
    )


class TestUnprovenHopAssociatesByDefault:
    @pytest.mark.parametrize("mode", MODES)
    async def test_rootless_returns_ossies_numbers(self, dialect, mode) -> None:
        async with ossie_engine(dialect) as engine:
            resp = await engine.execute(query(dimensions=["customers.region"], metrics=REV_ROOTLESS, **mode))
        assert cells(resp, "region", "rev") == AMOUNT_BY_REGION
        assert kinds(resp, "associated")
        assert not kinds(resp, "broadcast")

    @pytest.mark.parametrize("mode", MODES)
    async def test_rooted_at_orders(self, dialect, mode) -> None:
        async with ossie_engine(dialect) as engine:
            resp = await engine.execute(query(
                population="orders", dimensions=["customers.region"], metrics=REV_ROOTED, **mode))
        assert cells(resp, "region", "rev") == AMOUNT_BY_REGION_ROOTED
        assert kinds(resp, "associated")

    async def test_explicit_broadcast_repeats_the_total(self, dialect) -> None:
        async with ossie_engine(dialect) as engine:
            resp = await engine.execute(query(
                dimensions=["customers.region"], metrics=REV_ROOTLESS, to_many_handling="broadcast"))
        assert cells(resp, "region", "rev") == dict.fromkeys(AMOUNT_BY_REGION, AMOUNT_TOTAL)
        assert kinds(resp, "broadcast")


class TestKeylessHome:
    @pytest.mark.parametrize("mode", MODES)
    async def test_matches_the_keyed_model(self, dialect, mode) -> None:
        q = query(population="orders", dimensions=["orders.status"], metrics=CUSTOMER_MEASURES, **mode)
        async with ossie_engine(dialect, datasets=ossie_datasets(keyed=True)) as engine:
            keyed = await engine.execute(q.model_copy(update={"to_many_handling": "associate"}))
        async with ossie_engine(dialect) as engine:
            keyless = await engine.execute(q)
        assert cells(keyless, "status", "sp") == cells(keyed, "status", "sp")
        assert cells(keyless, "status", "ws") == cells(keyed, "status", "ws")
        assert cells(keyless, "status", "sp") == SPEND_BY_STATUS
        assert cells(keyless, "status", "ws") == WSUM_BY_STATUS
        assert cells(keyless, "status", "wa") == pytest.approx(WAVG_BY_STATUS)
        assert kinds(keyless, "associated")

    async def test_explicit_broadcast_still_broadcasts(self, dialect) -> None:
        async with ossie_engine(dialect) as engine:
            resp = await engine.execute(query(
                population="orders", dimensions=["orders.status"], metrics=[metric("sum(customers.spend)", "sp")],
                to_many_handling="broadcast"))
        assert cells(resp, "status", "sp") == {"completed": 100.0, "pending": 100.0}

    async def test_error_mode_refuses(self, dialect) -> None:
        q = query(population="orders", dimensions=["orders.status"], metrics=[metric("sum(customers.spend)", "sp")],
                  to_many_handling="error")
        async with ossie_engine(dialect) as engine:
            with pytest.raises(SlayerError):
                await engine.execute(q)


class TestRowIdentity:
    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("nullable", [True, False], ids=["nullable-unique", "all-non-null"])
    @pytest.mark.parametrize("source", ["sales", "tags"], ids=["local", "cross-model"])
    async def test_nullable_unique_keeps_each_sale(self, dialect, mode, nullable, source) -> None:
        stmts = sales_statements(nullable=nullable)
        async with seeded_exec_engine(dialect=dialect, seed=seed_with(dialect, stmts), datasets=[]) as (engine, _):
            ds = await engine.storage.get_datasource("test")
            assert ds is not None
            models = await engine.ingest(ds)
            sales = next(model for model in models if model.name == "sales")
            assert next(col for col in sales.fields if col.name == "source_ref").unique
            sales.relationships = [DatasetRelationship(to="tags", from_columns=["tag_id"], to_columns=["tag_id"])]
            for model in models:
                await engine.storage.save_dataset(model, _validate=False)
            resp = await engine.execute(sales_query(source=source, mode=mode))
        assert_sales_cells(resp)

    @pytest.mark.parametrize("mode", MODES)
    async def test_duplicate_rows_count_separately_fan_out_copies_once(self, dialect, mode) -> None:
        async with ossie_engine(dialect, duplicate=True) as engine:
            resp = await engine.execute(query(
                population="orders", dimensions=["orders.status"], metrics=[metric("count(customers.*)", "n")],
                **mode))
        assert cells(resp, "status", "n") == DUP_COUNT_BY_STATUS

    @pytest.mark.parametrize("mode", MODES)
    async def test_fan_out_under_a_joined_filter_counts_each_row_once(self, dialect, mode) -> None:
        async with ossie_engine(dialect) as engine:
            resp = await engine.execute(query(
                population="customers", dimensions=["orders.status"], metrics=[metric("sum(customers.spend)", "sp")],
                filter=["orders.amount >= 50"], **mode))
        assert cells(resp, "status", "sp") == FILTERED_SPEND_BY_STATUS

    @pytest.mark.parametrize("mode", MODES)
    async def test_filtered_keyless_home_numbers_its_filtered_rows(self, dialect, mode) -> None:
        models = ossie_datasets()
        models[1].filters = ["spend >= 20"]
        async with ossie_engine(dialect, datasets=models) as engine:
            resp = await engine.execute(query(
                population="orders", dimensions=["orders.status"], metrics=[metric("sum(customers.spend)", "sp")],
                **mode))
        assert cells(resp, "status", "sp") == {"completed": 30.0, "pending": 20.0}

    async def test_a_regroup_prefixed_home_column_does_not_shadow_the_row_identity(self, dialect) -> None:
        models = ossie_datasets()
        models[1].fields = [*models[1].fields, DatasetField(name="__regroup__row_id", sql="id", datatype=DataType.INTEGER)]
        async with ossie_engine(dialect, datasets=models) as engine:
            resp = await engine.execute(query(
                population="orders", dimensions=["orders.status"], metrics=[metric("sum(customers.spend)", "sp")]))
        assert cells(resp, "status", "sp") == SPEND_BY_STATUS

    def test_no_model_column_can_take_the_row_identity_name(self) -> None:
        with pytest.raises(ValidationError):
            DatasetField(name=ROW_IDENTITY_FIELD, datatype=DataType.INTEGER)

    async def test_postgres_executes(self, request) -> None:
        async with ossie_engine("postgres", request=request, duplicate=True) as engine:
            resp = await engine.execute(query(
                population="orders", dimensions=["orders.status"], metrics=[metric("count(customers.*)", "n")]))
        assert cells(resp, "status", "n") == DUP_COUNT_BY_STATUS

    @pytest.mark.parametrize("target", ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake",
                                        "clickhouse", "trino", "redshift", "databricks", "spark", "oracle"])
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
    """A primary key, not a nullable unique field, deduplicates the home."""

    async def test_sql_is_not_numbered(self) -> None:
        sales, tags = keyed_sales_datasets()
        sql = await _engine_generate(
            query=sales_query(source="sales", mode={}),
            dataset=sales, extra_datasets=[tags], dialect="duckdb", validate=False,
        )
        assert not ROW_NUMBER.search(sql), sql

    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("source", ["sales", "tags"], ids=["local", "cross-model"])
    async def test_null_unique_rows_contribute(self, dialect, mode, source) -> None:
        stmts = sales_statements(nullable=True, keyed=True)
        async with seeded_exec_engine(
            dialect=dialect, seed=seed_with(dialect, stmts), datasets=keyed_sales_datasets(),
        ) as (engine, _):
            resp = await engine.execute(sales_query(source=source, mode=mode))
        assert_sales_cells(resp)


class TestAttributableKeylessHome:
    """No dimension needs association, so a keyless home takes the plain path in every mode."""

    @pytest.fixture(params=["sqlite", "duckdb"])
    async def engine(self, request):
        models = keyless_root_datasets()
        models[0].relationships = [j.model_copy(update={"cardinality": RelationshipCardinality.MANY_TO_ONE})
                           if j.to == "customers" else j for j in models[0].relationships]
        async for engine in dev1841_engine(request, datasets=models):
            yield engine

    async def test_same_values_as_broadcast_without_association(self, engine) -> None:
        wavg = [metric("weighted_avg(customers.spend, weight=sum(orders.amount, partition_by=customers.regions.name))",
                        "wv")]
        kw = {"population": "orders", "dimensions": ["customers.tier"], "metrics": wavg}
        assoc = await engine.execute(query(**kw, to_many_handling="associate"))
        bcast = await engine.execute(query(**kw, to_many_handling="broadcast"))
        assert cells(assoc, "tier", "wv") == cells(bcast, "tier", "wv")
        assert not kinds(assoc, "associated")
        assert not kinds(assoc, "broadcast")
