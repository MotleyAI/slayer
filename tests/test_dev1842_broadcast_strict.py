"""DEV-1842 task 1.4 — broadcast/strict semantics are inherited, not re-built.

A dotted saved-measure reference expands to ordinary cross-model aggregates, so
it inherits DEV-1836 broadcast metadata and strict-mode errors verbatim. Every
assertion compares the dotted spelling against its hand-expanded twin: identical
broadcast warnings in lenient mode, identical failure under ``strict=True``.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError

from tests._dev1842_fixtures import broadcast_warnings, make_exec_engine, q

DOTTED = "customers.aov"
HAND = "customers.spend:sum / customers.*:count"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _warn_signature(resp) -> list:
    """Order-independent signature of the broadcast warnings: each aggregate's
    named measure plus the set of dimensions it broadcast across."""
    return sorted(
        (w.measure, tuple(sorted(d.dimension for d in w.dimensions)))
        for w in broadcast_warnings(resp)
    )


class TestBroadcastMetadataInherited:
    async def test_unattributable_dim_broadcasts_identically(self, exec_backend):
        _, engine = exec_backend
        dotted = await engine.execute(
            q(dimensions=["status"], measures=[{"formula": DOTTED, "name": "x"}]),
        )
        hand = await engine.execute(
            q(dimensions=["status"], measures=[{"formula": HAND, "name": "x"}]),
        )
        assert _warn_signature(dotted) == _warn_signature(hand)
        assert _warn_signature(dotted), "expected a broadcast warning over status"

    async def test_attributable_grain_never_warns(self, exec_backend):
        _, engine = exec_backend
        dotted = await engine.execute(
            q(dimensions=["customers.tier"],
              measures=[{"formula": DOTTED, "name": "x"}]),
        )
        assert broadcast_warnings(dotted) == []

    async def test_explicit_partition_broadcast_never_warns(self, exec_backend):
        """``customers.spend_grand`` (= ``spend:sum(partition_by=[])``) is an
        explicit broadcast producer — it neither warns nor errors, matching the
        hand-written ``partition_by=[]`` form."""
        _, engine = exec_backend
        resp = await engine.execute(
            q(dimensions=["status"],
              measures=[{"formula": "customers.spend_grand", "name": "x"}]),
        )
        assert broadcast_warnings(resp) == []


class TestStrictInherited:
    async def test_strict_broadcast_errors_like_hand(self, exec_backend):
        _, engine = exec_backend
        dotted_query = q(strict=True, dimensions=["status"],
                         measures=[{"formula": DOTTED, "name": "x"}])
        hand_query = q(strict=True, dimensions=["status"],
                       measures=[{"formula": HAND, "name": "x"}])
        with pytest.raises((SlayerError, ValueError)) as dotted_err:
            await engine.execute(dotted_query)
        with pytest.raises((SlayerError, ValueError)) as hand_err:
            await engine.execute(hand_query)
        # Same failure: same exception type, same broadcast dimension, same remedy.
        assert type(dotted_err.value) is type(hand_err.value)
        for err in (dotted_err.value, hand_err.value):
            message = str(err)
            assert "status" in message
            assert "cardinality" in message or "unique" in message

    async def test_strict_passes_when_attributable(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(
            q(strict=True, dimensions=["customers.tier"],
              measures=[{"formula": DOTTED, "name": "x"}]),
        )
        assert any(r.get("orders.x") is not None for r in resp.data)
