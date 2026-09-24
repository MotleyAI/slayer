"""Stage outputs carry their provable uniqueness: sibling stand-ins and query-backed models, executed."""

from __future__ import annotations

import pytest

from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.enums import DataType
from slayer.core.query import SlayerQuery
from slayer.engine.join_safety import provably_to_one
from tests._dev1836_fixtures import (
    AMOUNT_BY_TIER,
    AMOUNT_TOTAL,
    broadcast_warnings,
    customers_model,
    make_exec_engine,
)


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


def _flags(model: SlayerModel) -> dict:
    return {c.name: (c.primary_key, c.unique) for c in model.columns}


C_BY_ID = {
    "name": "c", "source_model": "customers", "dimensions": ["id"],
    "measures": [{"formula": "tier:max", "name": "tr"}],
}
C_BY_ID_TIER = {
    "name": "c", "source_model": "customers", "dimensions": ["id", "tier"],
    "measures": [{"formula": "spend:sum", "name": "sp"}],
}
ORDERS_JOIN_C = {
    "source_name": "orders",
    "joins": [{"target_model": "c", "join_pairs": [["customer_id", "id"]]}],
}
TIERS_BY_ID = {1: "gold", 2: "silver", 3: "gold", 4: "bronze"}


class TestSiblingJoins:
    async def test_join_onto_sibling_grain_is_proven(self, engine) -> None:
        resp = await engine.execute(_stages(C_BY_ID, {
            "source_model": ORDERS_JOIN_C, "dimensions": ["c.tr"],
            "measures": [{"formula": "amount:sum", "name": "amt"}],
        }))
        assert _by(resp, "orders.c.tr", "orders.amt") == pytest.approx(AMOUNT_BY_TIER)
        assert broadcast_warnings(resp) == []

    async def test_join_onto_sibling_grain_through_a_chain(self, engine) -> None:
        b = {
            "name": "b", "source_model": ORDERS_JOIN_C, "dimensions": ["c.tr"],
            "measures": [{"formula": "amount:sum", "name": "amt"}],
        }
        root = {
            "source_model": "b", "dimensions": ["c__tr"],
            "measures": [{"formula": "amt:sum", "name": "total"}],
        }
        resp = await engine.execute(_stages(C_BY_ID, b, root))
        assert _by(resp, "b.c__tr", "b.total") == pytest.approx(AMOUNT_BY_TIER)
        assert broadcast_warnings(resp) == []

    async def test_join_on_part_of_composite_sibling_grain_broadcasts(self, engine) -> None:
        resp = await engine.execute(_stages(C_BY_ID_TIER, {
            "source_model": ORDERS_JOIN_C, "dimensions": ["c.tier"],
            "measures": [{"formula": "amount:sum", "name": "amt"}],
        }))
        assert len(broadcast_warnings(resp)) == 1
        assert {r["orders.amt"] for r in resp.data} == {AMOUNT_TOTAL}

    async def test_extension_over_sibling_keeps_its_grain(self, engine) -> None:
        # The extension adds a column and a join; orders → c is the inverted hop onto c's grain.
        ext = {
            "source_name": "c",
            "columns": [{"name": "tr_up", "sql": "UPPER(tr)", "type": "string"}],
            "joins": [{"target_model": "orders", "join_pairs": [["id", "customer_id"]]}],
        }
        resp = await engine.execute(_stages(C_BY_ID, {
            "source_model": ext, "dimensions": ["tr_up"],
            "measures": [{"formula": "orders.amount:sum", "name": "amt"}],
        }))
        assert _by(resp, "c.tr_up", "c.amt") == pytest.approx(
            {"GOLD": 40.0, "SILVER": 30.0, "BRONZE": 40.0},
        )
        assert broadcast_warnings(resp) == []

    async def test_composite_sibling_grain_member_stays_aggregatable(self, engine) -> None:
        x = {**C_BY_ID_TIER, "name": "x"}
        resp = await engine.execute(_stages(x, {
            "source_model": "x", "dimensions": ["id"],
            "measures": [{"formula": "tier:max", "name": "tr"}],
        }))
        assert _by(resp, "x.id", "x.tr") == TIERS_BY_ID


class TestQueryBacked:
    async def test_composite_grain_member_stays_aggregatable(self, engine) -> None:
        model = await engine.create_model_from_query(SlayerQuery.model_validate({
            "source_model": "customers", "dimensions": ["id", "tier"],
            "measures": [{"formula": "spend:sum", "name": "sp"}],
        }), "vm_id_tier")
        assert _flags(model) == {"id": (True, False), "tier": (True, False), "sp": (False, False)}
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "vm_id_tier", "measures": [{"formula": "tier:max", "name": "mt"}],
        }))
        assert resp.data[0]["vm_id_tier.mt"] == "silver"

    async def test_single_column_grain_is_unique_not_identifier(self, engine) -> None:
        model = await engine.create_model_from_query(SlayerQuery.model_validate({
            "source_model": "customers", "dimensions": ["tier"],
            "measures": [{"formula": "*:count", "name": "n"}],
        }), "vm_tier")
        assert _flags(model) == {"tier": (False, True), "n": (False, False)}
        join = ModelJoin(target_model="vm_tier", join_pairs=[["tier", "tier"]])
        assert provably_to_one(edge=join, target_model=model)
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "vm_tier", "measures": [{"formula": "tier:max", "name": "mt"}],
        }))
        assert resp.data[0]["vm_tier.mt"] == "silver"

    async def test_metric_across_unique_grain_join_is_exact(self, engine) -> None:
        await engine.create_model_from_query(SlayerQuery.model_validate({
            "source_model": "customers", "dimensions": ["tier"],
            "measures": [{"formula": "*:count", "name": "n"}],
        }), "vm_tier_n")
        host = customers_model()
        host.name = "customers_t"
        host.joins.append(ModelJoin(target_model="vm_tier_n", join_pairs=[["tier", "tier"]]))
        await engine.storage.save_model(host, _validate=False)
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "customers_t", "dimensions": ["tier"],
            "measures": [{"formula": "vm_tier_n.n:sum", "name": "n"}],
        }))
        assert _by(resp, "customers_t.tier", "customers_t.n") == {"gold": 2, "silver": 1, "bronze": 1}
        assert broadcast_warnings(resp) == []

    async def test_measure_is_never_stamped(self, engine) -> None:
        model = await engine.create_model_from_query(SlayerQuery.model_validate({
            "source_model": "orders", "dimensions": ["status", "channel"],
            "measures": [{"formula": "sum(amount, partition_by=status)", "name": "st"}],
        }), "vm_st")
        assert _flags(model) == {
            "status": (True, False), "channel": (True, False), "st": (False, False),
        }


class TestAggregateValuedGrainMember:
    """Grain ``(status, ct)``: a join on ``status`` alone is unproven, never multiplied."""

    async def _model(self, engine) -> SlayerModel:
        return await engine.create_model_from_query(SlayerQuery.model_validate({
            "source_model": "orders",
            "dimensions": ["status", {"expression": "sum(amount, partition_by=channel)", "name": "ct"}],
        }), "vm_ctot")

    async def test_computed_dimension_is_stamped_into_the_grain(self, engine) -> None:
        model = await self._model(engine)
        assert _flags(model) == {"status": (True, False), "ct": (True, False)}
        join = ModelJoin(target_model="vm_ctot", join_pairs=[["status", "status"]])
        assert not provably_to_one(edge=join, target_model=model)

    async def test_join_on_status_alone_broadcasts_not_multiplies(self, engine) -> None:
        await self._model(engine)
        host = SlayerModel(
            name="two_ok", data_source="test",
            sql="SELECT 1 AS id, 'ok' AS status UNION ALL SELECT 2 AS id, 'ok' AS status",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
            ],
            joins=[ModelJoin(target_model="vm_ctot", join_pairs=[["status", "status"]])],
        )
        await engine.storage.save_model(host, _validate=False)
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "two_ok", "dimensions": ["vm_ctot.status"],
            "measures": [{"formula": "*:count", "name": "n"}],
        }))
        assert len(broadcast_warnings(resp)) == 1
        assert resp.data
        assert {r["two_ok.n"] for r in resp.data} == {2}
