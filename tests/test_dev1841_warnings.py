"""DEV-1841 task 2.3 / 3.5 — warnings both ways: the corrected fanning reason,
the unconditional dice–slice hint on broadcast payloads, the ``associated``
warning for non-additive cells, and the response-only informational entry for
semi-join-pushed filters (amending DEV-1840's silence).

Spec: openspec …/specs/queries/semantics — "Loud degradation"; and
queries/cross-model-aggregates — "Broadcast metadata", "Producer filter
routing".
"""

from __future__ import annotations

import warnings as _warnings

import pytest

from tests._dev1841_fixtures import (
    ASSOC_SPEND_BY_STATUS,
    ModelMeasure,
    SlayerQuery,
    assoc_q,
    associated_warnings,
    broadcast_warnings,
    make_exec_engine,
    pushed_filter_infos,
    q,
    status_key,
)

CM = ModelMeasure(formula="customers.spend:sum", name="cm")


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


class TestBroadcastReason:
    async def test_fanning_dimension_not_reported_unreachable(self, exec_backend):
        """Scenario: fanning dimension names the hop, not unreachability — the
        orders-level ``status`` is reachable from customers over the reverse
        (fanning) hop, so its reason must not say 'unreachable'."""
        _, engine = exec_backend
        resp = await engine.execute(q(dimensions=["status"], measures=[CM]))
        (w,) = broadcast_warnings(resp)
        reason = " ".join(d.reason for d in w.dimensions).lower()
        assert "unreachable" not in reason
        assert "fan" in reason or "hop" in reason or "many" in reason


class TestDiceSliceHint:
    @pytest.mark.parametrize("filters", [None, ["channel = 'app'"]])
    async def test_broadcast_carries_hint_unconditionally(
        self, exec_backend, filters,
    ):
        """Scenario: broadcast warning carries the dice–slice hint — present
        whether or not the query contains a filter, naming associate mode."""
        _, engine = exec_backend
        kw = {"dimensions": ["status"], "measures": [CM]}
        if filters:
            kw["filters"] = filters
        resp = await engine.execute(q(**kw))
        (w,) = broadcast_warnings(resp)
        hint = getattr(w, "hint", None)
        assert hint, "broadcast payload must carry a dice–slice hint field"
        assert "associate" in hint.lower()


class TestAssociatedWarning:
    async def test_implicit_associate_warns_both_channels(self, exec_backend):
        """Scenario: associate warns about overlapping populations — a payload
        naming the metric and the non-additive dimension, plus a Python-level
        warning."""
        _, engine = exec_backend
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            resp = await engine.execute(
                assoc_q(dimensions=["status"], measures=[CM]))
        (w,) = associated_warnings(resp)
        assert "cm" in w.measure or "spend" in w.measure
        assert "status" in " ".join(getattr(w, "dimensions", []) or []) \
            or "status" in w.human_message()
        assert caught, "the associated event must also surface as a Python warning"

    async def test_explicit_partition_suppresses_the_warning(self, exec_backend):
        """Explicit ``partition_by=`` is requested grain and must NOT warn —
        yet still carries the correct per-cell distinct-entity values."""
        _, engine = exec_backend
        resp = await engine.execute(assoc_q(
            dimensions=["status"],
            measures=[ModelMeasure(
                formula="customers.spend:sum(partition_by=status)", name="cm")]))
        assert associated_warnings(resp) == []
        by = status_key(resp)
        for status, spend in ASSOC_SPEND_BY_STATUS.items():
            assert float(by[(status,)]["orders.cm"]) == pytest.approx(spend)


class TestPushedFilterInformational:
    @pytest.mark.parametrize("mode", ["broadcast", "error"])
    async def test_pushed_filter_carries_info_in_every_mode(self, exec_backend, mode):
        """Scenario: pushed filter carries the informational entry, in any mode
        — a response-only entry naming the aggregate and filter, no Python
        warning, and never an error (error mode included)."""
        _, engine = exec_backend
        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            resp = await engine.execute(SlayerQuery(
                source_model="orders", dimensions=["customers.tier"],
                measures=[CM], filters=["channel = 'app'"],
                to_many_handling=mode))
        (info,) = pushed_filter_infos(resp)
        assert "channel" in info.human_message() or "channel" in str(
            info.model_dump())
        assert caught == []
