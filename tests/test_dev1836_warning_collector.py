"""DEV-1836 task 1.12 — the warning collector walks the regroup IR (design D6,
F7): one warning per distinct dropped filter / broadcast aggregate, across
producers, roles, and nesting.
"""

from __future__ import annotations

import pytest

from slayer.core.query import OrderItem

from tests._dev1836_fixtures import (
    AMOUNT_BY_BAND,
    ModelMeasure,
    SPEND_BAND,
    SPEND_BY_BAND,
    broadcast_warnings,
    dropped_filter_warnings,
    make_exec_engine,
    q,
    rows_by,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")
POP = ModelMeasure(formula="customers.regions.pop:sum", name="pop")


class TestDroppedFilterDedup:
    async def test_same_filter_dropped_by_two_producers_warns_once(
        self, exec_backend,
    ):
        """F7 — both the spend and the pop producer exclude the host filter;
        identity is (location, filter text), so ONE warning."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["customers.tier"], measures=[M, CM, POP],
            filters=["channel = 'app'"],
        ))
        dropped = dropped_filter_warnings(resp)
        assert len(dropped) == 1
        assert "channel" in dropped[0].filter_text

    async def test_nested_producers_share_the_dropped_filter_warning(
        self, exec_backend,
    ):
        """The computed dimension's nested producer and the cross-model
        measure's producer both drop the host conjunct — still one warning,
        and the metric values stay unfanned/unfiltered."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=[{"expression": SPEND_BAND, "name": "sband"}],
            measures=[M, CM],
            filters=["channel = 'app'"],
        ))
        assert len(dropped_filter_warnings(resp)) == 1
        by = rows_by(resp, "orders.sband")
        # Spine: app orders only (o2, o4, o5 — all gold customers → band hi).
        assert set(by) == {("hi",)}
        assert float(by[("hi",)]["orders.m"]) == pytest.approx(30.0)
        assert float(by[("hi",)]["orders.cm"]) == pytest.approx(
            SPEND_BY_BAND["hi"],
        )

    async def test_warning_arising_only_in_a_nested_producer_surfaces(
        self, exec_backend,
    ):
        """F7 traversal proof: the ONLY producer here is the computed
        dimension's (rooted at regions); the sole measure is local and drops
        nothing. A collector that never walks nested plans reports nothing."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=[{
                "expression": ("CASE WHEN customers.regions.pop:sum("
                               "partition_by=customers.regions.name) > 150 "
                               "THEN 'hi' ELSE 'lo' END"),
                "name": "pband",
            }],
            measures=[M],
            filters=["customers.tier = 'gold'"],
        ))
        # tier is unreachable from the regions-rooted producer → dropped there
        # (it still restricts the result spine to gold customers' orders).
        dropped = dropped_filter_warnings(resp)
        assert len(dropped) == 1
        assert "tier" in dropped[0].filter_text
        by = rows_by(resp, "orders.pband")
        # Gold orders: o1/o2 (c1, North pop 100 → lo), o4/o5 (c3, South → hi).
        assert set(by) == {("hi",), ("lo",)}
        assert float(by[("lo",)]["orders.m"]) == pytest.approx(30.0)
        assert float(by[("hi",)]["orders.m"]) == pytest.approx(10.0)


class TestBroadcastDedup:
    async def test_roles_dedup_but_distinct_aggregates_do_not(self, exec_backend):
        """One broadcast warning per distinct aggregate: cm appears in three
        roles (measure, filter, order) and pop in one — two warnings total."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"], measures=[M, CM, POP],
            filters=["customers.spend:sum > 0"],
            order=[OrderItem(column="customers.spend:sum", direction="desc")],
        ))
        warnings = broadcast_warnings(resp)
        assert len(warnings) == 2
        named = {w.measure for w in warnings}
        assert any("cm" in name for name in named), named
        assert any("pop" in name for name in named), named

    async def test_nested_and_outer_broadcasts_both_surface(self, exec_backend):
        """The collector walks nested producer plans: a broadcast inside the
        computed dimension's producer chain must not shadow the outer one."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=[{"expression": SPEND_BAND, "name": "sband"}, "status"],
            measures=[M, CM],
        ))
        # status is unattributable for cm → one broadcast for cm; the band
        # grain member itself is attributable (customers-rooted band).
        warnings = broadcast_warnings(resp)
        assert len(warnings) == 1
        (w,) = warnings
        assert "cm" in w.measure
        dims = {d.dimension for d in w.dimensions}
        assert any("status" in d for d in dims), dims
        assert not any("sband" in d for d in dims), dims

    async def test_solo_band_query_reference_values(self, exec_backend):
        """Anchor for the nested case: without status, nothing broadcasts."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=[{"expression": SPEND_BAND, "name": "sband"}],
            measures=[M, CM],
        ))
        assert broadcast_warnings(resp) == []
        by = rows_by(resp, "orders.sband")
        for band in ("hi", "lo"):
            assert float(by[(band,)]["orders.m"]) == pytest.approx(
                AMOUNT_BY_BAND[band],
            )
            assert float(by[(band,)]["orders.cm"]) == pytest.approx(
                SPEND_BY_BAND[band],
            )
