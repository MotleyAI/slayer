"""DEV-1868 tasks 1.1/1.2 — guard-bypassed characterization (design D1).

Recorded outcome, driving tasks 3.1/3.2: with ``check_partitioned_measures``
no-opped, every W1 and W2 shape compiles AND executes with correct values on
the existing substrate — no stage fails, so the lift is exactly the removal of
the two checker raises. (W3 needs no bypass: the AGGREGATE-phase seam raise is
already unreachable — recorded by tests/test_dev1868_composite_cm_exec.py.)
The bypass is ``raising=False`` so this file survives the checker's deletion.
"""

from __future__ import annotations

import pytest

import slayer.engine.compile.stages as stages_mod

from tests._dev1836_fixtures import ModelMeasure, make_exec_engine, month_td, q
from tests._dev1868_fixtures import (
    CM_PART_TIER,
    CUMSUM_PART_BY_TIER_MONTH,
    LAST_SPEND_BY_TIER,
    LAST_TIER,
    keyed_by_month,
)

TIER = "orders.customers.tier"


@pytest.fixture(params=["sqlite"])
async def bypassed_engine(request, monkeypatch):
    monkeypatch.setattr(stages_mod, "check_partitioned_measures",
                        lambda **kw: None, raising=False)
    async for engine in make_exec_engine(request):
        yield engine


class TestW1SubstrateIsComplete:
    async def test_ranked_measure_executes_end_to_end(self, bypassed_engine) -> None:
        resp = await bypassed_engine.execute(q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula=LAST_TIER, name="l")],
        ))
        got = {r[TIER]: r["orders.l"] for r in resp.data}
        for tier, expected in LAST_SPEND_BY_TIER.items():
            if expected is None:
                assert got[tier] is None
            else:
                assert float(got[tier]) == pytest.approx(expected)

    async def test_filter_and_order_positions_execute(self, bypassed_engine) -> None:
        filtered = await bypassed_engine.execute(q(
            dimensions=["customers.tier"],
            filters=[f"{LAST_TIER} > 100"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
        ))
        assert [r[TIER] for r in filtered.data] == ["silver"]
        ordered = await bypassed_engine.execute(q(
            dimensions=["customers.tier"],
            filters=["customers.tier is not null"],
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            order=[{"column": LAST_TIER, "direction": "desc"}],
        ))
        assert [r[TIER] for r in ordered.data] == ["silver", "gold", "bronze"]


class TestW2SubstrateIsComplete:
    async def test_transform_over_partitioned_inner_executes(
        self, bypassed_engine,
    ) -> None:
        resp = await bypassed_engine.execute(q(
            dimensions=["customers.tier"], time_dimensions=month_td(),
            measures=[ModelMeasure(formula=f"cumsum({CM_PART_TIER})", name="c")],
        ))
        by = keyed_by_month(resp, TIER)
        assert set(by) == set(CUMSUM_PART_BY_TIER_MONTH)
        for key, expected in CUMSUM_PART_BY_TIER_MONTH.items():
            cell = by[key]["orders.c"]
            if expected is None:
                assert cell is None
            else:
                assert float(cell) == pytest.approx(expected)
