"""Join keys name columns logically; the physical spelling is applied once, at
emission (system P16). Executed on SQLite and DuckDB over a graph whose every
key column is renamed (``tests/_dev1902_fixtures.py``)."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.join_walker import edges_between
from slayer.core.keys import ColumnKey, Grain
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.cardinality import CardinalityVerdict
from slayer.engine.join_safety import (
    _unique_key_sets,
    grain_determines,
    provably_to_one,
)
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1902_fixtures import (
    AMOUNT_BY_CUSTOMER,
    AMOUNT_BY_REGION,
    AMOUNT_BY_TIER,
    ASSOC_CREDIT_BY_STATUS,
    CREDIT_WITH_NEW_ORDER,
    cells,
    customers_model,
    make_exec_engine,
    orders_model,
    regions_model,
    renamed_graph,
    warnings_of,
)

AMT = ModelMeasure(formula="sum(amount)", name="amt")


@pytest.fixture(params=["sqlite", "duckdb"])
async def fk_renamed(request):
    """Renamed source FK only; the target PK keeps its physical name."""
    async for engine in make_exec_engine(
            request, models=renamed_graph(rename_fk=True, rename_pk=False)):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def pk_renamed(request):
    """Renamed target PK only; the source FK keeps its physical name."""
    async for engine in make_exec_engine(
            request, models=renamed_graph(rename_fk=False, rename_pk=True)):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def all_renamed(request):
    async for engine in make_exec_engine(request, models=renamed_graph()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def none_renamed(request):
    """Control: keys spelled physically on both sides (valid before and after)."""
    async for engine in make_exec_engine(
            request, models=renamed_graph(rename_fk=False, rename_pk=False)):
        yield engine


class TestExecutedRenamedKeys:
    async def test_control_physical_spelling_executes(self, none_renamed):
        resp = await none_renamed.execute(SlayerQuery(
            source_model="orders", dimensions=["customers.name"], measures=[AMT]))
        assert cells(resp, dim_suffix="customers.name", measure="amt") == AMOUNT_BY_CUSTOMER

    async def test_renamed_source_fk(self, fk_renamed):
        resp = await fk_renamed.execute(SlayerQuery(
            source_model="orders", dimensions=["customers.name"], measures=[AMT]))
        assert cells(resp, dim_suffix="customers.name", measure="amt") == AMOUNT_BY_CUSTOMER
        assert "cust_fk" in resp.sql

    async def test_renamed_target_pk_proves_the_hop(self, pk_renamed):
        resp = await pk_renamed.execute(SlayerQuery(
            source_model="orders", dimensions=["customers.tier"], measures=[AMT]))
        assert cells(resp, dim_suffix="customers.tier", measure="amt") == AMOUNT_BY_TIER
        assert warnings_of(resp, "broadcast") == []
        assert "customer_pk" in resp.sql

    async def test_renamed_both_sides_two_hops(self, all_renamed):
        resp = await all_renamed.execute(SlayerQuery(
            source_model="orders", dimensions=["customers.regions.name"],
            measures=[AMT]))
        assert cells(resp, dim_suffix="regions.name", measure="amt") == AMOUNT_BY_REGION
        assert warnings_of(resp, "broadcast") == []

    async def test_association_over_renamed_pk_root(self, all_renamed):
        resp = await all_renamed.execute(SlayerQuery(
            source_model="orders", dimensions=["status"],
            measures=[ModelMeasure(formula="sum(customers.credit)", name="cr")],
            to_many_handling="associate"))
        assert cells(resp, dim_suffix="status", measure="cr") == ASSOC_CREDIT_BY_STATUS

    async def test_renamed_fk_dimension_slices_target_measure(self, all_renamed):
        # The FK dimension reroots onto the target key, so it slices, not broadcasts.
        resp = await all_renamed.execute(SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["customer_id"],
            "measures": [{"formula": "sum(customers.credit)", "name": "cr"}]}))
        assert cells(resp, dim_suffix="customer_id", measure="cr") == {
            1: 100.0, 2: 200.0, 3: 300.0}
        assert warnings_of(resp, "broadcast") == []

    async def test_population_pushdown_over_renamed_keys(self, all_renamed):
        resp = await all_renamed.execute(SlayerQuery(
            source_model="customers",
            measures=[ModelMeasure(formula="sum(credit)", name="cr")],
            filters=["orders.status = 'new'"]))
        (row,) = resp.data
        value = next(v for k, v in row.items() if k.endswith("cr"))
        assert float(value) == pytest.approx(CREDIT_WITH_NEW_ORDER)
        assert warnings_of(resp, "semi_join_pushed")
        assert "cust_fk" in resp.sql
        assert "customer_pk" in resp.sql


class TestDetectionProfilesRenamedKeys:
    async def test_detects_many_to_one(self, all_renamed: SlayerQueryEngine):
        report = await all_renamed.detect_join_cardinality(
            data_source="test", model="orders")
        (finding,) = [f for f in report.findings if f.target_model == "customers"]
        assert finding.verdict is not CardinalityVerdict.SKIPPED_UNSUPPORTED
        assert finding.detected is JoinCardinality.MANY_TO_ONE


class TestProfilingSql:
    def test_mixed_case_physical_key_is_quoted(self):
        rows_sql, dist_sql = SlayerQueryEngine._side_stats_sql(
            table="customers", key_cols=["CustPK"], sqlglot_name="postgres")
        assert '"CustPK"' in rows_sql
        assert '"CustPK"' in dist_sql


class TestJoinSafetyLogicalSpace:
    def test_unique_key_sets_are_column_names(self):
        assert _unique_key_sets(customers_model()) == [["id"]]

    def test_renamed_target_pk_proves_to_one(self):
        orders, customers = orders_model(), customers_model()
        (edge,) = edges_between(source=orders, target=customers)
        assert edge.cardinality is None
        assert provably_to_one(edge=edge, target_model=customers)

    def test_renamed_pk_seeds_entity(self):
        customers = customers_model()
        assert grain_determines(
            key=ColumnKey(leaf="tier"), grain=Grain.of([ColumnKey(leaf="id")]),
            host_model=customers, models_by_name={"customers": customers})

    def test_renamed_fk_seeds_to_one_hop(self):
        # FK ``region_id`` (physical ``region_fk``) onto a renamed PK.
        customers, regions = customers_model(), regions_model()
        assert grain_determines(
            key=ColumnKey(path=("regions",), leaf="name"),
            grain=Grain.of([ColumnKey(leaf="region_id")]),
            host_model=customers,
            models_by_name={"customers": customers, "regions": regions})

    def test_fk_seed_ignores_a_physical_spelling_match(self):
        # A grain leaf spelled like the FK's physical column names no column.
        hub = SlayerModel(
            name="hub", data_source="test", sql_table="hub",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region_fk", sql="region_code", type=DataType.INT),
                Column(name="region_id", sql="region_fk", type=DataType.INT),
            ],
            joins=[ModelJoin(target_model="regions",
                             join_pairs=[["region_id", "id"]])],
        )
        regions = regions_model()
        assert not grain_determines(
            key=ColumnKey(path=("regions",), leaf="name"),
            grain=Grain.of([ColumnKey(leaf="region_fk")]),
            host_model=hub, models_by_name={"hub": hub, "regions": regions})
