"""Stage outputs carry their provable uniqueness: sibling stand-ins and query-backed datasets, executed."""

from __future__ import annotations

import pytest

from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.enums import DataType
from slayer.core.query import SlayerQuery
from slayer.core.relationship_walker import provably_to_one
from tests._dev1836_fixtures import (
    AMOUNT_BY_TIER,
    AMOUNT_TOTAL,
    broadcast_warnings,
    customers_dataset,
    make_exec_engine,
)
from slayer.engine.query_engine import SlayerQueryEngine


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for eng in make_exec_engine(request):
        yield eng


def _stages(*bodies) -> list[SlayerQuery]:
    return [SlayerQuery.model_validate(b) for b in bodies]


def _by(resp, key: str, value: str) -> dict:
    out = {r[key]: r[value] for r in resp.data}
    assert len(out) == len(resp.data)
    return out


def _flags(dataset: SlayerDataset) -> dict:
    return {c.name: (c.primary_key, c.unique) for c in dataset.fields}


C_BY_ID = {
    "name": "c", "population": "customers", "dimensions": ["customers.id"],
    "metrics": [{"metric": "max(customers.tier)", "alias": "tr"}],
}
C_BY_ID_TIER = {
    "name": "c", "population": "customers", "dimensions": ["customers.id", "customers.tier"],
    "metrics": [{"metric": "sum(customers.spend)", "alias": "sp"}],
}
ORDERS_RELATIONSHIP_C = {
    "dataset": "orders",
    "relationships": [{"to": "c", "from_columns": ["customer_id"], "to_columns": ["id"]}],
}
TIERS_BY_ID = {1: "gold", 2: "silver", 3: "gold", 4: "bronze"}


class TestSiblingJoins:
    async def test_join_onto_sibling_grain_is_proven(self, engine) -> None:
        resp = await engine.execute(_stages(C_BY_ID, {
            "population": "orders", "extensions": [ORDERS_RELATIONSHIP_C], "dimensions": ["c.tr"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}],
        }))
        assert _by(resp, "c.tr", "amt") == pytest.approx(AMOUNT_BY_TIER)
        assert broadcast_warnings(resp) == []

    async def test_join_onto_sibling_grain_through_a_chain(self, engine) -> None:
        b = {
            "name": "b", "population": "orders", "extensions": [ORDERS_RELATIONSHIP_C], "dimensions": ["c.tr"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}],
        }
        root = {
            "population": "b", "dimensions": ["b.c__tr"],
            "metrics": [{"metric": "sum(b.amt)", "alias": "total"}],
        }
        resp = await engine.execute(_stages(C_BY_ID, b, root))
        assert _by(resp, "b.c__tr", "total") == pytest.approx(AMOUNT_BY_TIER)
        assert broadcast_warnings(resp) == []

    async def test_join_on_part_of_composite_sibling_grain_broadcasts(self, engine) -> None:
        resp = await engine.execute(_stages(C_BY_ID_TIER, {
            "population": "orders", "extensions": [ORDERS_RELATIONSHIP_C], "dimensions": ["c.tier"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}], "to_many_handling": "broadcast",
        }))
        assert len(broadcast_warnings(resp)) == 1
        assert {r["amt"] for r in resp.data} == {AMOUNT_TOTAL}

    async def test_join_on_part_of_composite_sibling_grain_associates_by_default(self, engine) -> None:
        final = {"population": ORDERS_RELATIONSHIP_C, "dimensions": ["c.tier"],
                 "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}]}
        default = await engine.execute(_stages(C_BY_ID_TIER, final))
        assoc = await engine.execute(_stages(C_BY_ID_TIER, {**final, "to_many_handling": "associate"}))
        assert sorted(map(str, default.data)) == sorted(map(str, assoc.data))
        assert [w for w in default.warnings or [] if w.kind == "associated"]
        assert broadcast_warnings(default) == []

    async def test_extension_over_sibling_keeps_its_grain(self, engine) -> None:
        # The extension adds a relationship; orders → c is the inverted hop onto c's grain.
        ext = {
            "dataset": "c",
            "relationships": [{"to": "orders", "from_columns": ["id"], "to_columns": ["customer_id"]}],
        }
        resp = await engine.execute(_stages(C_BY_ID, {
            "population": "c", "extensions": [ext],
            "dimensions": [{"field": "upper(c.tr)", "alias": "tr_up"}],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}],
        }))
        assert _by(resp, "tr_up", "amt") == pytest.approx(
            {"GOLD": 40.0, "SILVER": 30.0, "BRONZE": 40.0},
        )
        assert broadcast_warnings(resp) == []

    async def test_composite_sibling_grain_member_stays_aggregatable(self, engine) -> None:
        x = {**C_BY_ID_TIER, "name": "x"}
        resp = await engine.execute(_stages(x, {
            "population": "x", "dimensions": ["x.id"],
            "metrics": [{"metric": "max(x.tier)", "alias": "tr"}],
        }))
        assert _by(resp, "x.id", "tr") == TIERS_BY_ID


class TestQueryBacked:
    async def test_composite_grain_member_stays_aggregatable(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(SlayerQuery.model_validate({
            "population": "customers", "dimensions": ["customers.id", "customers.tier"],
            "metrics": [{"metric": "sum(customers.spend)", "alias": "sp"}],
        }), "vm_id_tier")
        assert _flags(model) == {"id": (True, False), "tier": (True, False), "sp": (False, False)}
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "vm_id_tier", "metrics": [{"metric": "max(vm_id_tier.tier)", "alias": "mt"}],
        }))
        assert resp.data[0]["mt"] == "silver"

    async def test_single_column_grain_is_unique_not_identifier(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(SlayerQuery.model_validate({
            "population": "customers", "dimensions": ["customers.tier"],
            "metrics": [{"metric": "count(customers.*)", "alias": "n"}],
        }), "vm_tier")
        assert _flags(model) == {"tier": (False, True), "n": (False, False)}
        join = DatasetRelationship(to="vm_tier", from_columns=["tier"], to_columns=["tier"])
        assert provably_to_one(edge=join, target_dataset=model)
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "vm_tier", "metrics": [{"metric": "max(vm_tier.tier)", "alias": "mt"}],
        }))
        assert resp.data[0]["mt"] == "silver"

    async def test_metric_across_unique_grain_join_is_exact(self, engine: SlayerQueryEngine) -> None:
        await engine.create_dataset_from_query(SlayerQuery.model_validate({
            "population": "customers", "dimensions": ["customers.tier"],
            "metrics": [{"metric": "count(customers.*)", "alias": "n"}],
        }), "vm_tier_n")
        host = customers_dataset()
        host.name = "customers_t"
        host.relationships.append(DatasetRelationship(to="vm_tier_n", from_columns=["tier"], to_columns=["tier"]))
        await engine.storage.save_dataset(host, _validate=False)
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "customers_t", "dimensions": ["customers_t.tier"],
            "metrics": [{"metric": "sum(vm_tier_n.n)", "alias": "n"}],
        }))
        assert _by(resp, "customers_t.tier", "n") == {"gold": 2, "silver": 1, "bronze": 1}
        assert broadcast_warnings(resp) == []

    async def test_measure_is_never_stamped(self, engine: SlayerQueryEngine) -> None:
        model = await engine.create_dataset_from_query(SlayerQuery.model_validate({
            "population": "orders", "dimensions": ["orders.status", "orders.channel"],
            "metrics": [{"metric": "sum(orders.amount, partition_by=orders.status)", "alias": "st"}],
        }), "vm_st")
        assert _flags(model) == {
            "status": (True, False), "channel": (True, False), "st": (False, False),
        }


class TestAggregateValuedGrainMember:
    """Grain ``(status, ct)``: a relationship on ``status`` alone is unproven, never multiplied."""

    async def _dataset(self, engine: SlayerQueryEngine) -> SlayerDataset:
        return await engine.create_dataset_from_query(SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status", {"field": "sum(orders.amount, partition_by=orders.channel)", "alias": "ct"}],
        }), "vm_ctot")

    async def test_computed_dimension_is_stamped_into_the_grain(self, engine) -> None:
        model = await self._dataset(engine)
        assert _flags(model) == {"status": (True, False), "ct": (True, False)}
        join = DatasetRelationship(to="vm_ctot", from_columns=["status"], to_columns=["status"])
        assert not provably_to_one(edge=join, target_dataset=model)

    async def test_join_on_status_alone_broadcasts_not_multiplies(self, engine: SlayerQueryEngine) -> None:
        await self._dataset(engine)
        host = SlayerDataset(
            name="two_ok", data_source="test",
            sql="SELECT 1 AS id, 'ok' AS status UNION ALL SELECT 2 AS id, 'ok' AS status",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="status", datatype=DataType.STRING),
            ],
            relationships=[DatasetRelationship(to="vm_ctot", from_columns=["status"], to_columns=["status"])],
        )
        await engine.storage.save_dataset(host, _validate=False)
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "two_ok", "dimensions": ["vm_ctot.status"],
            "metrics": [{"metric": "count(two_ok.*)", "alias": "n"}], "to_many_handling": "broadcast",
        }))
        assert len(broadcast_warnings(resp)) == 1
        assert resp.data
        assert {r["n"] for r in resp.data} == {2}
