"""DEV-1868 W3 — cross-model aggregate operands inside composites, executed
values (spec: queries/cross-model-aggregates, compose-in-expressions
requirement). Characterization (task 1.2): every shape below already routes to
the combined SELECT — the AGGREGATE-phase seam raise is unreachable from user
queries, so task 3.3 flips it to an internal invariant."""

from __future__ import annotations

import pytest

from tests._dev1836_fixtures import (
    AMOUNT_BY_BAND,
    ModelMeasure,
    SPEND_BAND,
    SPEND_BY_BAND,
    make_exec_engine,
    q,
    rows_by,
)
from tests._dev1868_fixtures import (
    RATIO_BY_TIER,
    ROUND_BY_TIER,
    SPEND_OVER_POP_BY_REGION,
    SUM_MINUS_MAX_BY_TIER,
    TIMES2_BY_TIER,
)

TIER = "orders.customers.tier"
REGION = "orders.customers.regions.name"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _assert_by_tier(resp, *, name: str, expected: dict) -> None:
    by = rows_by(resp, TIER)
    assert len(resp.data) == len(expected)
    for tier, value in expected.items():
        cell = by[(tier,)][name]
        if value is None:
            assert cell is None
        else:
            assert float(cell) == pytest.approx(value)


class TestCompositeShapes:
    async def test_local_and_cross_model_in_one_expression(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula="amount:sum / customers.spend:sum",
                                   name="r")],
        ))
        _assert_by_tier(resp, name="orders.r", expected=RATIO_BY_TIER)

    async def test_scalar_call_wrapping_a_cross_model_operand(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(
                formula="round(customers.spend:sum / amount:sum, 2)", name="r")],
        ))
        _assert_by_tier(resp, name="orders.r", expected=ROUND_BY_TIER)

    async def test_cross_model_operand_with_literal(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula="customers.spend:sum * 2", name="r")],
        ))
        _assert_by_tier(resp, name="orders.r", expected=TIMES2_BY_TIER)

    async def test_two_cross_model_operands_same_model(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(
                formula="customers.spend:sum - customers.spend:max", name="r")],
        ))
        _assert_by_tier(resp, name="orders.r", expected=SUM_MINUS_MAX_BY_TIER)

    async def test_two_cross_model_operands_different_models(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(q(
            dimensions=["customers.regions.name"],
            measures=[ModelMeasure(
                formula="customers.spend:sum / customers.regions.pop:sum",
                name="r")],
        ))
        by = rows_by(resp, REGION)
        for region, value in SPEND_OVER_POP_BY_REGION.items():
            cell = by[(region,)]["orders.r"]
            if value is None:
                assert cell is None
            else:
                assert float(cell) == pytest.approx(value)


class TestComputedDimensionCoexistence:
    async def test_band_dimension_with_cross_model_measure(self, exec_engine) -> None:
        resp = await exec_engine.execute(q(
            dimensions=[{"expression": SPEND_BAND, "name": "band"}],
            measures=[ModelMeasure(formula="amount:sum", name="s"),
                      ModelMeasure(formula="customers.spend:sum", name="sp")],
        ))
        by = rows_by(resp, "orders.band")
        for band in ("hi", "lo"):
            assert float(by[(band,)]["orders.s"]) == pytest.approx(
                AMOUNT_BY_BAND[band])
            assert float(by[(band,)]["orders.sp"]) == pytest.approx(
                SPEND_BY_BAND[band])
