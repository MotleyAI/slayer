"""DEV-1900 — a derived fanning dimension routes through the mode axis.

A dimension naming a derived column whose definition crosses the 1:N
``regions → region_events`` hop is unattributable exactly as a structural
fanning dimension is: broadcast (default, warning naming the hop, the metric
repeated), associate (per-cell over distinct root entities), or error. An
explicit ``partition_by=`` on it is the hard error outside associate.

Covers ``queries/attribution-modes`` (Query-level mode selection).
"""

from __future__ import annotations

import pytest

from tests._dev1900_fixtures import (
    AMOUNT_SUM,
    ASSOC_AMOUNT_BY_BAD_POP,
    ASSOC_AMOUNT_NORTH_FAN_DEFECT,
    ASSOC_SPEND_BY_BAD_POP,
    ASSOC_SPEND_NORTH_FAN_DEFECT,
    BROADCAST_AMOUNT_TOTAL,
    BROADCAST_SPEND_TOTAL,
    BAD_POP,
    ModelMeasure,
    SPEND_SUM,
    associated_warnings,
    bad_pop_vals,
    broadcast_warnings,
    make_exec_engine,
    orders_q,
)

HOP = "region_events"


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _names_hop(warnings) -> bool:
    for w in warnings:
        if HOP in w.human_message():
            return True
        if HOP in " ".join((d.reason or "") for d in getattr(w, "dimensions", [])):
            return True
    return False


class TestBroadcast:
    async def test_broadcasts_the_metric_with_a_warning_naming_the_hop(self, engine):
        resp = await engine.execute(orders_q(
            dimensions=[BAD_POP], measures=[AMOUNT_SUM, SPEND_SUM]))
        amt = bad_pop_vals(resp, "orders.amt")
        spend = bad_pop_vals(resp, "orders.csp")
        # Every cell carries the ungrouped total (broadcast), never the per-event
        # multiplied figure.
        for cell in amt:
            assert float(amt[cell]) == pytest.approx(BROADCAST_AMOUNT_TOTAL), cell
            assert float(spend[cell]) == pytest.approx(BROADCAST_SPEND_TOTAL), cell
        assert amt[150.0] != pytest.approx(ASSOC_AMOUNT_NORTH_FAN_DEFECT)
        assert spend[150.0] != pytest.approx(ASSOC_SPEND_NORTH_FAN_DEFECT)
        bw = broadcast_warnings(resp)
        assert bw, "a derived fanning dimension must broadcast with a warning"
        assert _names_hop(bw), "the broadcast warning must name the fanning hop"


class TestAssociate:
    @pytest.mark.xfail(strict=True, reason=(
        "DEV-1910: a cross-model aggregate associated over an unattributable "
        "dimension must home-root to count home entities absent from the query "
        "population (the zero-order customer); the association producer is still "
        "host-rooted. Remove this marker when DEV-1910 lands."))
    async def test_associates_over_distinct_entities_per_cell(self, engine):
        resp = await engine.execute(orders_q(
            dimensions=[BAD_POP], measures=[AMOUNT_SUM, SPEND_SUM],
            to_many_handling="associate"))
        amt = bad_pop_vals(resp, "orders.amt")
        spend = bad_pop_vals(resp, "orders.csp")
        for cell, expected in ASSOC_AMOUNT_BY_BAD_POP.items():
            assert float(amt[cell]) == pytest.approx(expected), cell
        for cell, expected in ASSOC_SPEND_BY_BAD_POP.items():
            assert float(spend[cell]) == pytest.approx(expected), cell
        assert amt[150.0] != pytest.approx(ASSOC_AMOUNT_NORTH_FAN_DEFECT)
        assert spend[150.0] != pytest.approx(ASSOC_SPEND_NORTH_FAN_DEFECT)
        assert associated_warnings(resp), "associate cells are non-additive"


class TestErrorMode:
    """Local and cross-model refusals proven separately — a combined query could
    stop at the first metric and never exercise the other."""

    async def test_local_metric_refuses(self, engine):
        with pytest.raises(ValueError) as ei:
            await engine.execute(orders_q(
                dimensions=[BAD_POP], measures=[AMOUNT_SUM], to_many_handling="error"))
        assert "bad_pop" in str(ei.value), ei.value

    async def test_cross_model_metric_refuses(self, engine):
        with pytest.raises(ValueError) as ei:
            await engine.execute(orders_q(
                dimensions=[BAD_POP], measures=[SPEND_SUM], to_many_handling="error"))
        assert "bad_pop" in str(ei.value), ei.value


class TestExplicitPartitionBy:
    @pytest.mark.parametrize("mode", ["broadcast", "error"])
    async def test_partition_by_fanning_derived_is_hard_error(self, engine, mode):
        """An explicit partition_by naming the fanning derived dimension is an
        error outside associate — every partition key must be attributable."""
        measure = ModelMeasure(
            formula="amount:sum(partition_by=customers.regions.bad_pop)", name="w")
        with pytest.raises(ValueError) as ei:
            await engine.execute(orders_q(
                dimensions=[BAD_POP], measures=[measure], to_many_handling=mode))
        msg = str(ei.value)
        assert HOP in msg or "attributable" in msg, msg
