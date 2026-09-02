"""DEV-1836 task 1.6 — broadcast metadata and strict mode (design D6/D9, F8).

Spec: openspec …/specs/queries/cross-model-aggregates — "Broadcast metadata",
"Strict mode". A silent-semantics event (implicit-grain broadcast, dropped
producer filter) is reported per distinct aggregate in lenient mode and errors
under ``SlayerQuery.strict``; explicit ``partition_by=`` broadcasting neither
warns nor errors.
"""

from __future__ import annotations

import warnings as _warnings

import pytest

from slayer.core.errors import BroadcastGrainWarning, SlayerError
from slayer.core.query import OrderItem

from tests._dev1836_fixtures import (
    ModelMeasure,
    SPEND_BY_TIER,
    SPEND_TOTAL,
    broadcast_warnings,
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


class TestBroadcastMetadata:
    async def test_broadcast_reported_per_metric_and_dimension(self, exec_backend):
        _, engine = exec_backend
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            resp = await engine.execute(q(dimensions=["status"], measures=[M, CM]))
        (w,) = broadcast_warnings(resp)
        assert "cm" in w.measure
        dims = {d.dimension for d in w.dimensions}
        assert any("status" in d for d in dims), dims
        assert all(d.reason for d in w.dimensions)
        # The local measure never warns.
        assert "m" != w.measure
        hits = [c for c in caught
                if issubclass(c.category, BroadcastGrainWarning)]
        assert len(hits) == 1

    async def test_only_the_unsafe_dimension_is_reported(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(
            q(dimensions=["customers.tier", "status"], measures=[M, CM]),
        )
        (w,) = broadcast_warnings(resp)
        dims = {d.dimension for d in w.dimensions}
        assert any("status" in d for d in dims), dims
        assert not any("tier" in d for d in dims), dims

    async def test_same_aggregate_in_several_roles_warns_once(self, exec_backend):
        """F8 — measure + filter + ORDER BY roles dedup to one warning."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"], measures=[M, CM],
            filters=["customers.spend:sum > 0"],
            order=[OrderItem(column="customers.spend:sum", direction="desc")],
        ))
        assert len(broadcast_warnings(resp)) == 1

    async def test_distinct_aggregates_warn_separately(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"],
            measures=[
                CM,
                ModelMeasure(formula="customers.regions.pop:sum", name="pop"),
            ],
        ))
        assert len(broadcast_warnings(resp)) == 2

    async def test_filter_only_use_warns_too(self, exec_backend):
        """A hidden (filter-only) aggregate use still reports its broadcast,
        named by its canonical aggregate form."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["status"], measures=[M],
            filters=["customers.spend:sum > 0"],
        ))
        (w,) = broadcast_warnings(resp)
        assert "spend" in w.measure

    async def test_attributable_grain_never_warns(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(
            q(dimensions=["customers.tier"], measures=[M, CM]),
        )
        assert broadcast_warnings(resp) == []

    async def test_explicit_partition_broadcast_never_warns(self, exec_backend):
        _, engine = exec_backend
        control = await engine.execute(q(dimensions=["status"], measures=[M]))
        resp = await engine.execute(q(
            dimensions=["status"],
            measures=[M, ModelMeasure(
                formula="customers.spend:sum(partition_by=[])", name="total",
            )],
        ))
        assert broadcast_warnings(resp) == []
        # The keyless (single-row producer) attach is cardinality-neutral.
        assert len(resp.data) == len(control.data) == 2
        for row in resp.data:
            assert float(row["orders.total"]) == pytest.approx(SPEND_TOTAL)

    async def test_broadcast_reasons_distinguish_hop_from_unreachable(
        self, exec_backend,
    ):
        """The per-dimension reason separates 'unproven/fanning hop' from
        'unreachable from the root' (no stored edge at all)."""
        _, engine = exec_backend
        # segments.label: a stored but unproven hop from customers.
        via_hop = await engine.execute(
            q(dimensions=["customers.segments.label"], measures=[CM]),
        )
        (w_hop,) = broadcast_warnings(via_hop)
        # tier from the regions-rooted pop aggregate: no stored edge.
        no_edge = await engine.execute(q(
            dimensions=["customers.tier"],
            measures=[ModelMeasure(formula="customers.regions.pop:sum",
                                   name="pop")],
        ))
        (w_edge,) = broadcast_warnings(no_edge)
        hop_reasons = " ".join(d.reason for d in w_hop.dimensions).lower()
        edge_reasons = " ".join(d.reason for d in w_edge.dimensions).lower()
        assert "unreachable" in edge_reasons
        assert "unreachable" not in hop_reasons


class TestStrictMode:
    async def test_strict_broadcast_errors_with_remedy(self, exec_backend):
        _, engine = exec_backend
        query = q(strict=True, dimensions=["status"], measures=[M, CM])
        with pytest.raises((SlayerError, ValueError)) as ei:
            await engine.execute(query)
        message = str(ei.value)
        assert "cm" in message or "spend" in message
        assert "status" in message
        # The remedy names the metadata fix.
        assert "cardinality" in message or "unique" in message

    async def test_strict_dropped_filter_errors(self, exec_backend):
        _, engine = exec_backend
        query = q(
            strict=True, dimensions=["customers.tier"], measures=[M, CM],
            filters=["channel = 'app'"],
        )
        with pytest.raises((SlayerError, ValueError)) as ei:
            await engine.execute(query)
        assert "channel" in str(ei.value)

    async def test_strict_passes_when_all_attributable(self, exec_backend):
        _, engine = exec_backend
        lenient = await engine.execute(
            q(dimensions=["customers.tier"], measures=[M, CM]),
        )
        strict = await engine.execute(
            q(strict=True, dimensions=["customers.tier"], measures=[M, CM]),
        )
        lenient_by = rows_by(lenient, "orders.customers.tier")
        strict_by = rows_by(strict, "orders.customers.tier")
        assert set(strict_by) == set(lenient_by)
        for key, row in strict_by.items():
            for col in ("orders.m", "orders.cm"):
                assert row[col] == lenient_by[key][col], (key, col)
        for tier, expected in SPEND_BY_TIER.items():
            assert float(strict_by[(tier,)]["orders.cm"]) == pytest.approx(expected)

    async def test_strict_allows_explicit_partition_broadcast(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(q(
            strict=True, dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:sum(partition_by=[])", name="total",
            )],
        ))
        for row in resp.data:
            assert float(row["orders.total"]) == pytest.approx(SPEND_TOTAL)
