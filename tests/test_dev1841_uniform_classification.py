"""DEV-1841 task 2.1 / 4.2 — local aggregates over a fanning dimension route
through the same attribution classification as cross-model ones, in every
consumer role. The naive join-multiplied value must be unrepresentable.

Spec: openspec …/specs/queries/semantics — "No double counting" (local
base-path hole), "Attribution by determination". Shape: rooted at customers,
``spend:sum`` by ``orders.status``; customer 1 has two ``ok`` orders, so a
naive join doubles c1 (100) in the ok cell (520 instead of 420).
"""

from __future__ import annotations

import pytest

from slayer.core.query import OrderItem

from tests._dev1841_fixtures import (
    ASSOC_LOCAL_SPEND_BY_STATUS,
    LOCAL_OK_FAN_DEFECT,
    LOCAL_SPEND_TOTAL,
    ModelMeasure,
    associated_warnings,
    broadcast_warnings,
    cust_q,
    make_exec_engine,
    status_key,
)

SP = ModelMeasure(formula="spend:sum", name="sp")


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _ok(resp) -> float:
    return float(status_key(resp, root="customers")[("ok",)]["customers.sp"])


class TestDefaultBroadcastsNeverMultiplies:
    async def test_dimensionless_total_is_all_customers(self, exec_backend):
        """Sanity oracle: the fan-free local total sums every customer once."""
        _, engine = exec_backend
        resp = await engine.execute(cust_q(measures=[SP]))
        assert float(resp.data[0]["customers.sp"]) == pytest.approx(
            LOCAL_SPEND_TOTAL)

    async def test_default_broadcasts_and_warns(self, exec_backend):
        """Scenario: local aggregate over a fanning dimension never multiplies —
        under the default it broadcasts (both cells equal) with a warning, never
        the fanned figure."""
        _, engine = exec_backend
        resp = await engine.execute(
            cust_q(dimensions=["orders.status"], measures=[SP]))
        by = status_key(resp, root="customers")
        vals = {k: float(v["customers.sp"]) for k, v in by.items()}
        assert vals[("ok",)] == pytest.approx(vals[("new",)])  # broadcast
        assert vals[("ok",)] != pytest.approx(LOCAL_OK_FAN_DEFECT)
        assert vals[("ok",)] != pytest.approx(ASSOC_LOCAL_SPEND_BY_STATUS["ok"])
        assert broadcast_warnings(resp)


class TestAssociateGivesDistinctEntityValue:
    async def test_associate_per_cell_never_fans(self, exec_backend):
        """Scenario (associate branch): the cell carries the distinct-entity
        value; c1 counts once in ok."""
        _, engine = exec_backend
        resp = await engine.execute(
            cust_q(dimensions=["orders.status"], measures=[SP],
                   to_many_handling="associate"))
        by = status_key(resp, root="customers")
        for status, spend in ASSOC_LOCAL_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["customers.sp"]) == pytest.approx(spend)
        assert _ok(resp) != pytest.approx(LOCAL_OK_FAN_DEFECT)
        assert associated_warnings(resp)


class TestEveryConsumerRoleAvoidsTheFan:
    """Scenario: fanned local aggregates are caught in every consumer context —
    each resolves per the mode exactly as a directly selected measure."""

    async def test_arithmetic_composite_leaf(self, exec_backend):
        _, engine = exec_backend
        composite = ModelMeasure(formula="spend:sum + 1", name="sp1")
        resp = await engine.execute(
            cust_q(dimensions=["orders.status"], measures=[composite],
                   to_many_handling="associate"))
        by = status_key(resp, root="customers")
        assert float(by[("ok",)]["customers.sp1"]) == pytest.approx(
            ASSOC_LOCAL_SPEND_BY_STATUS["ok"] + 1)
        assert float(by[("ok",)]["customers.sp1"]) != pytest.approx(
            LOCAL_OK_FAN_DEFECT + 1)

    async def test_aggregate_phase_filter_only(self, exec_backend):
        """``spend:sum`` used only in a HAVING-style filter (not selected): the
        threshold sees 420, not the fanned 520 that would keep ok anyway — a
        threshold BELOW the fan but ABOVE the true value discriminates."""
        _, engine = exec_backend
        star = ModelMeasure(formula="*:count", name="n")
        resp = await engine.execute(
            cust_q(dimensions=["orders.status"], measures=[star],
                   filters=["spend:sum > 450"],
                   to_many_handling="associate"))
        # Correct ok=420 fails > 450 → ok dropped; a fanned 520 would keep it.
        keys = set(status_key(resp, root="customers"))
        assert ("ok",) not in keys

    async def test_order_context(self, exec_backend):
        """``spend:sum`` present with an ORDER BY over it resolves via the mode,
        not the fan: the ok cell carries 420, never the multiplied 520."""
        _, engine = exec_backend
        resp = await engine.execute(
            cust_q(dimensions=["orders.status"], measures=[SP],
                   order=[OrderItem(column="spend:sum", direction="desc")],
                   to_many_handling="associate"))
        # ok / new / a NULL-status cell for orderless c7 (rooted at customers).
        assert len(resp.data) == 3
        assert _ok(resp) == pytest.approx(ASSOC_LOCAL_SPEND_BY_STATUS["ok"])
        assert _ok(resp) != pytest.approx(LOCAL_OK_FAN_DEFECT)
