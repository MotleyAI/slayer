"""DEV-1836 task 1.12 — the warning collector walks the regroup IR (design D6,
F7): one warning per distinct dropped filter / broadcast aggregate, across
producers, roles, and nesting.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from slayer.core.errors import UnreachableFilterDroppedWarning
from slayer.core.query import OrderItem
from slayer.engine.query_engine import (
    _collect_broadcast_warnings,
    _collect_dropped_filter_warnings,
)

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


def _attach(**overrides):
    empty = SimpleNamespace(regroup_attach_plans=[], cross_model_aggregate_plans=[])
    base = dict(
        dropped_filter_warnings=[], broadcast_measure=None,
        broadcast_dimensions=[], producer_plan=empty,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _planned(attaches):
    return SimpleNamespace(
        regroup_attach_plans=list(attaches), cross_model_aggregate_plans=[],
    )


class TestCollectorIdentityUnits:
    """Direct collector units for the identity rules the engine shapes above
    cannot isolate: conjunct-level reason merging and per-stage broadcast
    identity."""

    def test_conjuncts_of_one_filter_merge_reasons_instead_of_raising(self):
        """Two conjuncts of ONE filter can drop for different reasons; that is
        not a planner inconsistency — one warning, reasons merged."""
        text = "a.x = 1 AND b.y = 2"
        plans = _planned([
            _attach(dropped_filter_warnings=[UnreachableFilterDroppedWarning(
                filter_text=text, reason="reason A",
            )]),
            _attach(dropped_filter_warnings=[UnreachableFilterDroppedWarning(
                filter_text=text, reason="reason B",
            )]),
        ])
        (w,) = _collect_dropped_filter_warnings(
            planned_list=[plans], stages=[SimpleNamespace(name=None)],
        )
        assert w.filter_text == text
        assert w.reason == "reason A; reason B"

    def test_agreeing_reasons_still_collapse_to_one(self):
        plans = _planned([
            _attach(dropped_filter_warnings=[UnreachableFilterDroppedWarning(
                filter_text="a.x = 1", reason="same",
            )]),
            _attach(dropped_filter_warnings=[UnreachableFilterDroppedWarning(
                filter_text="a.x = 1", reason="same",
            )]),
        ])
        (w,) = _collect_dropped_filter_warnings(
            planned_list=[plans], stages=[SimpleNamespace(name=None)],
        )
        assert w.reason == "same"

    def test_same_label_broadcasts_in_two_stages_stay_two_payloads(self):
        """D6 identity is per stage: same-labeled aggregates in different DAG
        stages are distinct — dimensions must not union across stages."""
        stage_one = _planned([_attach(
            broadcast_measure="orders.cm", broadcast_dimensions=[("d1", "r1")],
        )])
        stage_two = _planned([_attach(
            broadcast_measure="orders.cm", broadcast_dimensions=[("d2", "r2")],
        )])
        out = _collect_broadcast_warnings(
            planned_list=[stage_one, stage_two],
            stages=[SimpleNamespace(name="s1"), SimpleNamespace(name=None)],
        )
        assert len(out) == 2
        assert {w.location for w in out} == {"stage 's1'", "stages[1]"}
        by_location = {w.location: w for w in out}
        assert [d.dimension for d in by_location["stage 's1'"].dimensions] == ["d1"]
        assert [d.dimension for d in by_location["stages[1]"].dimensions] == ["d2"]

    def test_same_stage_same_measure_still_unions_dimensions(self):
        plans = _planned([
            _attach(broadcast_measure="orders.cm",
                    broadcast_dimensions=[("d1", "r1")]),
            _attach(broadcast_measure="orders.cm",
                    broadcast_dimensions=[("d1", "r1"), ("d2", "r2")]),
        ])
        (w,) = _collect_broadcast_warnings(
            planned_list=[plans], stages=[SimpleNamespace(name=None)],
        )
        assert [d.dimension for d in w.dimensions] == ["d1", "d2"]

    def test_broadcast_human_message_names_location_and_reasons(self):
        (w,) = _collect_broadcast_warnings(
            planned_list=[_planned([_attach(
                broadcast_measure="orders.cm",
                broadcast_dimensions=[("status", "crosses an unproven join hop to x")],
            )])],
            stages=[SimpleNamespace(name=None)],
        )
        message = w.human_message()
        assert "'orders.cm'" in message
        assert "stages[0]" in message
        assert "status (crosses an unproven join hop to x)" in message
