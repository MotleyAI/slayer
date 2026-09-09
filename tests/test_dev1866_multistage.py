"""DEV-1866 — per-stage population inference, end to end through engine.execute.

Design §1: inference runs for the main query and each named stage independently,
before bundle construction. A non-final stage may omit source_model and be
inferred; a stage whose dims anchor at a SIBLING stage's name fails closed with
the sibling-stage diagnostic (the engine must thread sibling names into inference).
"""

from __future__ import annotations

import pytest

from slayer.core.errors import PopulationErrorReason, PopulationInferenceError
from slayer.core.query import SlayerQuery

from tests._dev1866_fixtures import make_chain_exec_engine, make_inference_engine


async def test_nonfinal_stage_inferred_independently() -> None:
    """A named non-final stage omits source_model and is inferred (→ customers),
    then feeds the root — the whole DAG executes."""
    async for engine in make_chain_exec_engine("sqlite"):
        s1 = SlayerQuery(
            name="s1",
            dimensions=["customers.region"],
            measures=[{"formula": "orders.amount:sum", "name": "rev"}],
        )
        root = SlayerQuery(
            source_model="s1",
            measures=[{"formula": "rev:sum", "name": "total"}],
        )
        resp = await engine.execute([s1, root])
        assert resp.data  # executed end to end ⇒ s1's population was inferred
        break


async def test_sibling_anchored_stage_fails_closed_via_execute() -> None:
    """The final stage omits source_model and anchors a dim at sibling ``s1``."""
    engine = await make_inference_engine()
    s1 = SlayerQuery(
        name="s1",
        source_model="orders",
        dimensions=["status"],
        measures=[{"formula": "amount:sum", "name": "rev"}],
    )
    root = SlayerQuery(dimensions=["s1.status"])  # rootless, anchored at the sibling
    with pytest.raises(PopulationInferenceError) as ei:
        await engine.execute([s1, root])
    assert ei.value.reason is PopulationErrorReason.SIBLING_STAGE
    assert "s1" in str(ei.value)
