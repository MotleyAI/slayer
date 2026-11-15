"""Keyless homes associate through a per-query row identity; unproven hops associate by default
(spec: queries/attribution-modes › Per-query row identity for keyless homes, Mode selection and the
association default)."""

from __future__ import annotations

import re

import pytest
from pydantic import ValidationError
from pytest_postgresql import factories

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.errors import SlayerError
from slayer.core.models import Column, ModelJoin
from slayer.sql.naming import ROW_IDENTITY_COLUMN
from tests._dev1841_fixtures import keyless_root_models
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
    _seed,
    cells,
    kinds,
    measure,
    ossie_engine,
    ossie_models,
    query,
)
from tests._engine_helpers import _engine_generate, seeded_exec_engine

postgresql_proc = factories.postgresql_proc(port=None)

MODES = [pytest.param({}, id="default"), pytest.param({"to_many_handling": "associate"}, id="associate")]
REV_ROOTLESS = [measure("sum(orders.amount)", "rev")]
REV_ROOTED = [measure("sum(amount)", "rev")]
CUSTOMER_MEASURES = [
    measure("sum(customers.spend)", "sp"),
    measure("wsum(customers.spend)", "ws"),
    measure("weighted_avg(customers.spend, weight=customers.spend)", "wa"),
]
#: Dialects whose ``ROW_NUMBER()`` rejects an empty window.
ORDERED_ROW_NUMBER = {"tsql", "snowflake", "databricks", "spark", "oracle"}
ROW_NUMBER = re.compile(r"ROW_NUMBER\s*\(\s*\)\s*OVER\s*\(([^)]*)\)", re.IGNORECASE)


@pytest.fixture(params=["sqlite", "duckdb"])
def dialect(request) -> str:
    return request.param


class TestUnprovenHopAssociatesByDefault:
    @pytest.mark.parametrize("mode", MODES)
    async def test_rootless_returns_ossies_numbers(self, dialect, mode) -> None:
        async with ossie_engine(dialect) as engine:
            resp = await engine.execute(query(dimensions=["customers.region"], measures=REV_ROOTLESS, **mode))
        assert cells(resp, "region", "rev") == AMOUNT_BY_REGION
        assert kinds(resp, "associated")
        assert not kinds(resp, "broadcast")

    @pytest.mark.parametrize("mode", MODES)
    async def test_rooted_at_orders(self, dialect, mode) -> None:
        async with ossie_engine(dialect) as engine:
            resp = await engine.execute(query(
                source_model="orders", dimensions=["customers.region"], measures=REV_ROOTED, **mode))
        assert cells(resp, "region", "rev") == AMOUNT_BY_REGION_ROOTED
        assert kinds(resp, "associated")

    async def test_explicit_broadcast_repeats_the_total(self, dialect) -> None:
        async with ossie_engine(dialect) as engine:
            resp = await engine.execute(query(
                dimensions=["customers.region"], measures=REV_ROOTLESS, to_many_handling="broadcast"))
        assert cells(resp, "region", "rev") == dict.fromkeys(AMOUNT_BY_REGION, AMOUNT_TOTAL)
        assert kinds(resp, "broadcast")


class TestKeylessHome:
    @pytest.mark.parametrize("mode", MODES)
    async def test_matches_the_keyed_model(self, dialect, mode) -> None:
        q = query(source_model="orders", dimensions=["status"], measures=CUSTOMER_MEASURES, **mode)
        async with ossie_engine(dialect, models=ossie_models(keyed=True)) as engine:
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
                source_model="orders", dimensions=["status"], measures=[measure("sum(customers.spend)", "sp")],
                to_many_handling="broadcast"))
        assert cells(resp, "status", "sp") == {"completed": 100.0, "pending": 100.0}

    async def test_error_mode_refuses(self, dialect) -> None:
        q = query(source_model="orders", dimensions=["status"], measures=[measure("sum(customers.spend)", "sp")],
                  to_many_handling="error")
        async with ossie_engine(dialect) as engine:
            with pytest.raises(SlayerError):
                await engine.execute(q)


class TestRowIdentity:
    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("nullable", [True, False], ids=["nullable-unique", "all-non-null"])
    @pytest.mark.parametrize("source", ["sales", "tags"], ids=["local", "cross-model"])
    async def test_nullable_unique_keeps_each_sale(self, dialect, mode, nullable, source) -> None:
        refs = "(NULL, 1, 30, 1), (NULL, 1, 40, 2)" if nullable else "('b', 1, 30, 1), ('c', 1, 40, 2)"
        stmts = [
            "CREATE TABLE sales (source_ref VARCHAR UNIQUE, tag_id INTEGER, amount INTEGER, weight INTEGER)",
            "CREATE TABLE tags (tag_id INTEGER, label VARCHAR)",
            f"INSERT INTO sales VALUES ('a', 1, 10, 1), {refs}, ('d', 2, 90, 1)",
            "INSERT INTO tags VALUES (1, 'shared'), (1, 'shared'), (2, 'other')",
        ]
        async with seeded_exec_engine(dialect=dialect, seed=_seed(dialect, stmts), models=[]) as (engine, _):
            ds = await engine.storage.get_datasource("test")
            assert ds is not None
            models = await engine.ingest(ds)
            sales = next(model for model in models if model.name == "sales")
            assert next(col for col in sales.columns if col.name == "source_ref").unique
            sales.joins = [ModelJoin(target_model="tags", join_pairs=[["tag_id", "tag_id"]])]
            for model in models:
                await engine.storage.save_model(model, _validate=False)
            dimension = "tags.label" if source == "sales" else "label"
            prefix = "" if source == "sales" else "sales."
            resp = await engine.execute(query(
                source_model=source, dimensions=[dimension],
                measures=[
                    measure(f"sum({prefix}amount)", "total"),
                    measure("count(sales.*)", "n"),
                    measure(f"weighted_avg({prefix}amount, weight={prefix}weight)", "weighted"),
                ],
                **mode,
            ))
        assert cells(resp, "label", "total") == {"shared": 80.0, "other": 90.0}
        assert cells(resp, "label", "n") == {"shared": 3.0, "other": 1.0}
        assert cells(resp, "label", "weighted") == pytest.approx({"shared": 30.0, "other": 90.0})
        assert kinds(resp, "associated")

    @pytest.mark.parametrize("mode", MODES)
    async def test_duplicate_rows_count_separately_fan_out_copies_once(self, dialect, mode) -> None:
        async with ossie_engine(dialect, duplicate=True) as engine:
            resp = await engine.execute(query(
                source_model="orders", dimensions=["status"], measures=[measure("count(customers.*)", "n")],
                **mode))
        assert cells(resp, "status", "n") == DUP_COUNT_BY_STATUS

    @pytest.mark.parametrize("mode", MODES)
    async def test_fan_out_under_a_joined_filter_counts_each_row_once(self, dialect, mode) -> None:
        async with ossie_engine(dialect) as engine:
            resp = await engine.execute(query(
                source_model="customers", dimensions=["orders.status"], measures=[measure("sum(spend)", "sp")],
                filters=["orders.amount >= 50"], **mode))
        assert cells(resp, "status", "sp") == FILTERED_SPEND_BY_STATUS

    @pytest.mark.parametrize("mode", MODES)
    async def test_filtered_keyless_home_numbers_its_filtered_rows(self, dialect, mode) -> None:
        models = ossie_models()
        models[1].filters = ["spend >= 20"]
        async with ossie_engine(dialect, models=models) as engine:
            resp = await engine.execute(query(
                source_model="orders", dimensions=["status"], measures=[measure("sum(customers.spend)", "sp")],
                **mode))
        assert cells(resp, "status", "sp") == {"completed": 30.0, "pending": 20.0}

    async def test_a_regroup_prefixed_home_column_does_not_shadow_the_row_identity(self, dialect) -> None:
        models = ossie_models()
        models[1].columns = [*models[1].columns, Column(name="__regroup__row_id", sql="id", type=DataType.INT)]
        async with ossie_engine(dialect, models=models) as engine:
            resp = await engine.execute(query(
                source_model="orders", dimensions=["status"], measures=[measure("sum(customers.spend)", "sp")]))
        assert cells(resp, "status", "sp") == SPEND_BY_STATUS

    def test_no_model_column_can_take_the_row_identity_name(self) -> None:
        with pytest.raises(ValidationError):
            Column(name=ROW_IDENTITY_COLUMN, type=DataType.INT)

    async def test_postgres_executes(self, request) -> None:
        async with ossie_engine("postgres", request=request, duplicate=True) as engine:
            resp = await engine.execute(query(
                source_model="orders", dimensions=["status"], measures=[measure("count(customers.*)", "n")]))
        assert cells(resp, "status", "n") == DUP_COUNT_BY_STATUS

    @pytest.mark.parametrize("target", ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake",
                                        "clickhouse", "trino", "redshift", "databricks", "spark", "oracle"])
    async def test_every_dialect_numbers_the_home_rows(self, target) -> None:
        models = ossie_models()
        sql = await _engine_generate(
            query=query(source_model="orders", dimensions=["status"], measures=[measure("count(customers.*)", "n")]),
            model=models[0], extra_models=models[1:], dialect=target, validate=False,
        )
        over = ROW_NUMBER.findall(sql)
        assert over, sql
        if target in ORDERED_ROW_NUMBER:
            assert all("ORDER BY" in clause.upper() for clause in over), sql

    async def test_keyed_homes_are_not_numbered(self) -> None:
        models = ossie_models(keyed=True)
        sql = await _engine_generate(
            query=query(source_model="orders", dimensions=["status"], measures=[measure("count(customers.*)", "n")]),
            model=models[0], extra_models=models[1:], dialect="duckdb", validate=False,
        )
        assert not ROW_NUMBER.search(sql), sql


class TestAttributableKeylessHome:
    """No dimension needs association, so a keyless home takes the plain path in every mode."""

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
        assoc = await engine.execute(query(**kw, to_many_handling="associate"))
        bcast = await engine.execute(query(**kw, to_many_handling="broadcast"))
        assert cells(assoc, "tier", "wv") == cells(bcast, "tier", "wv")
        assert not kinds(assoc, "associated")
        assert not kinds(assoc, "broadcast")
