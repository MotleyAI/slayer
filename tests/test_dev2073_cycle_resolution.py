"""Resolution inside an ingested directed 3-cycle: literal edge first; rootless queries fail closed.

Pinned until the walker-consolidation issue (DEV-2074) revisits the routing rule.
"""

from __future__ import annotations

import pytest

from slayer.core.errors import PopulationErrorReason, PopulationInferenceError

from tests._dev2073_fixtures import DIRECTED_3_CYCLE, live


class TestLiteralEdgeFirst:
    async def test_token_binds_the_direct_fanning_edge(self) -> None:
        async with live(DIRECTED_3_CYCLE) as lv:
            await lv.ingest()
            resp = await lv.query(
                source_model="x", dimensions=["id"],
                measures=[{"formula": "z.amount:sum", "name": "s"}],
            )
            # Direct z.x_id edge: x2 owns both z rows; via y.z it would be {1: 10, 2: 5}.
            assert {r["x.id"]: r["x.s"] for r in resp.data} == {1: None, 2: 15.0}


class TestRootlessAcrossADirectedCycle:
    async def test_fails_closed_without_a_source_model(self) -> None:
        async with live(DIRECTED_3_CYCLE) as lv:
            await lv.ingest()
            with pytest.raises(PopulationInferenceError) as exc:
                await lv.query(dimensions=["x.v", "y.v", "z.v"])
            assert exc.value.reason is PopulationErrorReason.NO_VIABLE_CANDIDATE

    async def test_succeeds_with_a_source_model(self) -> None:
        async with live(DIRECTED_3_CYCLE) as lv:
            await lv.ingest()
            resp = await lv.query(source_model="x", dimensions=["v", "y.v", "z.v"])
            assert resp.data
