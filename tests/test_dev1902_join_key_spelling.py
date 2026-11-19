"""Join keys name columns logically; the physical spelling is applied once, at
emission (semantics Principle 3). Executed on SQLite and DuckDB over a graph whose every
key column is renamed (``tests/_dev1902_fixtures.py``)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import RelationshipKeyError
from slayer.core.relationship_walker import edges_between, provably_to_one, unique_key_sets
from slayer.core.keys import FieldKey, Grain
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import QueryMetric, SlayerQuery
from slayer.engine.cardinality.inference import CardinalityVerdict
from slayer.engine.cardinality.relationship_safety import grain_determines
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1902_fixtures import (
    AMOUNT_BY_CUSTOMER,
    AMOUNT_BY_REGION,
    AMOUNT_BY_TIER,
    ASSOC_CREDIT_BY_STATUS,
    CREDIT_WITH_NEW_ORDER,
    cells,
    customers_dataset,
    make_exec_engine,
    orders_dataset,
    regions_dataset,
    renamed_graph,
    warnings_of,
)
from slayer.sql.naming import key_identifier
from tests._engine_helpers import datasets_bundle

AMT = QueryMetric(metric="sum(orders.amount)", alias="amt")


def _query(**kw) -> SlayerQuery:
    return SlayerQuery.model_validate(kw)


@pytest.fixture(params=["sqlite", "duckdb"])
async def fk_renamed(request):
    """Renamed source FK only; the target PK keeps its physical name."""
    async for engine in make_exec_engine(
            request, datasets=renamed_graph(rename_fk=True, rename_pk=False)):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def pk_renamed(request):
    """Renamed target PK only; the source FK keeps its physical name."""
    async for engine in make_exec_engine(
            request, datasets=renamed_graph(rename_fk=False, rename_pk=True)):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def all_renamed(request) -> AsyncIterator[SlayerQueryEngine]:
    async for engine in make_exec_engine(request, datasets=renamed_graph()):
        yield engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def none_renamed(request):
    """Control: keys spelled physically on both sides (valid before and after)."""
    async for engine in make_exec_engine(
            request, datasets=renamed_graph(rename_fk=False, rename_pk=False)):
        yield engine


class TestExecutedRenamedKeys:
    async def test_control_physical_spelling_executes(self, none_renamed):
        resp = await none_renamed.execute(_query(
            population="orders", dimensions=["customers.name"], metrics=[AMT]))
        assert cells(resp, dim_suffix="customers.name", metric="amt") == AMOUNT_BY_CUSTOMER

    async def test_renamed_source_fk(self, fk_renamed):
        resp = await fk_renamed.execute(_query(
            population="orders", dimensions=["customers.name"], metrics=[AMT]))
        assert cells(resp, dim_suffix="customers.name", metric="amt") == AMOUNT_BY_CUSTOMER
        assert "cust_fk" in resp.sql

    async def test_renamed_target_pk_proves_the_hop(self, pk_renamed):
        resp = await pk_renamed.execute(_query(
            population="orders", dimensions=["customers.tier"], metrics=[AMT]))
        assert cells(resp, dim_suffix="customers.tier", metric="amt") == AMOUNT_BY_TIER
        assert warnings_of(resp, "broadcast") == []
        assert "customer_pk" in resp.sql

    async def test_renamed_both_sides_two_hops(self, all_renamed: SlayerQueryEngine):
        resp = await all_renamed.execute(_query(
            population="orders", dimensions=["customers.regions.name"],
            metrics=[AMT]))
        assert cells(resp, dim_suffix="regions.name", metric="amt") == AMOUNT_BY_REGION
        assert warnings_of(resp, "broadcast") == []

    async def test_association_over_renamed_pk_root(self, all_renamed: SlayerQueryEngine):
        resp = await all_renamed.execute(_query(
            population="orders", dimensions=["status"],
            metrics=[QueryMetric(metric="sum(customers.credit)", alias="cr")],
            to_many_handling="associate"))
        assert cells(resp, dim_suffix="status", metric="cr") == ASSOC_CREDIT_BY_STATUS

    async def test_renamed_fk_dimension_slices_target_measure(self, all_renamed: SlayerQueryEngine):
        # The FK dimension reroots onto the target key, so it slices, not broadcasts.
        resp = await all_renamed.execute(SlayerQuery.model_validate({
            "population": "orders", "dimensions": ["orders.customer_id"],
            "metrics": [{"metric": "sum(customers.credit)", "alias": "cr"}]}))
        assert cells(resp, dim_suffix="customer_id", metric="cr") == {
            1: 100.0, 2: 200.0, 3: 300.0}
        assert warnings_of(resp, "broadcast") == []

    async def test_population_pushdown_over_renamed_keys(self, all_renamed: SlayerQueryEngine):
        resp = await all_renamed.execute(_query(
            population="customers",
            metrics=[QueryMetric(metric="sum(customers.credit)", alias="cr")],
            filter=["orders.status = 'new'"]))
        (row,) = resp.data
        value = next(v for k, v in row.items() if k.endswith("cr"))
        assert float(value) == pytest.approx(CREDIT_WITH_NEW_ORDER)
        assert warnings_of(resp, "semi_join_pushed")
        assert resp.sql is not None
        assert "cust_fk" in resp.sql
        assert "customer_pk" in resp.sql


class TestDetectionProfilesRenamedKeys:
    async def test_detects_many_to_one(self, all_renamed: SlayerQueryEngine):
        report = await all_renamed.detect_relationship_cardinality(
            data_source="test", dataset="orders")
        (finding,) = [f for f in report.findings if f.to == "customers"]
        assert finding.verdict is not CardinalityVerdict.SKIPPED_UNSUPPORTED
        assert finding.detected is RelationshipCardinality.MANY_TO_ONE


def _side_stats_sql(**kwargs) -> tuple[str, str]:
    """The two profiling statements, rendered as ``_side_stats`` executes them."""
    rows, dist = SlayerQueryEngine._side_stats_sql(**kwargs)
    dialect = kwargs["sqlglot_name"]
    return rows.sql(dialect=dialect), dist.sql(dialect=dialect)


class TestProfilingSql:
    def test_mixed_case_physical_key_is_quoted(self):
        rows_sql, dist_sql = _side_stats_sql(
            table="customers", key_cols=[key_identifier("CustPK", quoted=False)], sqlglot_name="postgres")
        assert '"CustPK"' in rows_sql
        assert '"CustPK"' in dist_sql


class TestJoinSafetyLogicalSpace:
    def test_unique_key_sets_are_column_names(self):
        assert unique_key_sets(customers_dataset()) == [["id"]]

    def test_renamed_target_pk_proves_to_one(self):
        orders, customers = orders_dataset(), customers_dataset()
        (edge,) = edges_between(source=orders, target=customers)
        assert edge.cardinality is None
        assert provably_to_one(edge=edge, target_dataset=customers)

    def test_renamed_pk_seeds_entity(self):
        customers = customers_dataset()
        assert grain_determines(
            key=FieldKey(leaf="tier"), grain=Grain.of([FieldKey(leaf="id")]),
            host_dataset=customers, datasets_by_name={"customers": customers},
            bundle=datasets_bundle({"customers": customers}))

    def test_renamed_fk_seeds_to_one_hop(self):
        # FK ``region_id`` (physical ``region_fk``) onto a renamed PK.
        customers, regions = customers_dataset(), regions_dataset()
        assert grain_determines(
            key=FieldKey(path=("regions",), leaf="name"),
            grain=Grain.of([FieldKey(leaf="region_id")]),
            host_dataset=customers,
            datasets_by_name={"customers": customers, "regions": regions},
            bundle=datasets_bundle({"customers": customers, "regions": regions}))

    def test_fk_seed_ignores_a_physical_spelling_match(self):
        # A grain leaf spelled like the FK's physical column names no column.
        hub = SlayerDataset(
            name="hub", data_source="test", sql_table="hub",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="region_fk", sql="region_code", datatype=DataType.INTEGER),
                DatasetField(name="region_id", sql="region_fk", datatype=DataType.INTEGER),
            ],
            relationships=[DatasetRelationship(to="regions",
                             from_columns=["region_id"], to_columns=["id"])],
        )
        regions = regions_dataset()
        assert not grain_determines(
            key=FieldKey(path=("regions",), leaf="name"),
            grain=Grain.of([FieldKey(leaf="region_fk")]),
            host_dataset=hub, datasets_by_name={"hub": hub, "regions": regions},
            bundle=datasets_bundle({"hub": hub, "regions": regions}))


def _customer_totals(*, key: str = "customer_id") -> SlayerDataset:
    """Query-backed: its columns come from the backing query, so the join key is checked once populated."""
    return SlayerDataset.model_validate({
        "name": "customer_totals", "data_source": "test",
        "source_queries": [{"population": "orders", "dimensions": ["orders.customer_id"],
                            "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}]}],
        "relationships": [{"to": "customers", "from_columns": [key], "to_columns": ["id"]}],
    })


class TestQueryBackedJoinKeys:
    async def test_join_on_backing_output_column_executes(self, all_renamed: SlayerQueryEngine):
        await all_renamed.save_dataset(_customer_totals())
        resp = await all_renamed.execute(_query(
            population="customer_totals", dimensions=["customers.name"],
            metrics=[{"metric": "sum(amt)", "alias": "total"}]))
        assert cells(resp, dim_suffix="customers.name", metric="total") == AMOUNT_BY_CUSTOMER

    async def test_extension_join_over_query_backed_base_executes(self, all_renamed: SlayerQueryEngine):
        base = SlayerDataset.model_validate({
            **_customer_totals().model_dump(exclude={"joins"}), "relationships": []})
        await all_renamed.save_dataset(base)
        resp = await all_renamed.execute(_query(
            population="customer_totals",
            extensions=[{"dataset": "customer_totals", "relationships": [
                {"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}]}],
            dimensions=["customers.name"],
            metrics=[{"metric": "sum(amt)", "alias": "total"}]))
        assert cells(resp, dim_suffix="customers.name", metric="total") == AMOUNT_BY_CUSTOMER

    async def test_join_on_undeclared_output_rejected_at_save(self, all_renamed: SlayerQueryEngine):
        model = _customer_totals(key="nope")
        with pytest.raises(RelationshipKeyError, match="key 'nope'"):
            await all_renamed.save_dataset(model)
