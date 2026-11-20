"""Resolution inside an ingested directed 3-cycle: an anchor reads its only to-one path; rootless queries tie."""

from __future__ import annotations

import pytest

from slayer.core.errors import PopulationErrorReason, PopulationInferenceError

from tests._dev2073_fixtures import DIRECTED_3_CYCLE, live


class TestToOnePathFirst:
    async def test_anchor_reads_its_only_to_one_path(self) -> None:
        async with live(DIRECTED_3_CYCLE) as lv:
            await lv.ingest()
            resp = await lv.query(
                population="x", dimensions=["x.id"],
                metrics=[{"metric": "sum(z.amount)", "alias": "s"}],
            )
            # Routed from the home z by its only to-one path, the direct z.x_id edge (x2 owns both z rows);
            # the population's route x → y → z would give {1: 10, 2: 5}.
            assert {r["x.id"]: r["s"] for r in resp.data} == {1: None, 2: 15.0}


class TestRootlessAcrossADirectedCycle:
    async def test_ties_without_a_population(self) -> None:
        async with live(DIRECTED_3_CYCLE) as lv:
            await lv.ingest()
            with pytest.raises(PopulationInferenceError) as exc:
                await lv.query(dimensions=["x.v", "y.v", "z.v"])
            assert exc.value.reason is PopulationErrorReason.TIE

    async def test_succeeds_with_a_population(self) -> None:
        async with live(DIRECTED_3_CYCLE) as lv:
            await lv.ingest()
            resp = await lv.query(population="x", dimensions=["x.v", "y.v", "z.v"])
            assert resp.data
