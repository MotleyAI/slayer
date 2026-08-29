"""DEV-1824 stage-boundary contract — a computed dimension derived from
aggregation and banding crosses into downstream stages as a plain column, and
internal ``__regroup__`` placeholders never surface in result keys, response
metadata, or emitted SQL.

Covers the ``queries/computed-dimensions`` scenarios "Downstream stage consumes
a regrouped dimension" and "No internal names leak" (tasks.md 1.6).

Stage-1 rows (from the DEV-1824 fixture oracles, BAND35 = city total > 35):
(North, 0) bt=60 · (North, 1) bt=40 · (South, 1) bt=50 · (NULL, 1) bt=60.
"""

from __future__ import annotations

import pytest

from tests._dev1824_fixtures import (
    BAND35,
    ModelMeasure,
    SlayerQuery,
    make_exec_engine,
    q,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


def _stages(**stage2_kwargs) -> list:
    stage1 = q(
        name="banded",
        dimensions=["region", {"expression": BAND35, "name": "band"}],
        measures=[ModelMeasure(formula="amount:sum", name="bt")],
    )
    stage2 = SlayerQuery(source_model={"source_name": "banded"}, **stage2_kwargs)
    return [stage1, stage2]


class TestDownstreamStageConsumesRegroupedDimension:
    async def test_select_order_and_reaggregate_over_band(self, exec_engine) -> None:
        resp = await exec_engine.execute(_stages(
            dimensions=["band"],
            measures=[ModelMeasure(formula="bt:sum", name="total")],
            order=[{"column": "band", "direction": "desc"}],
        ))
        # Exact public result keys: upstream stage name + PUBLIC column names.
        assert resp.columns == ["banded.band", "banded.total"]
        got = [(int(r["banded.band"]), float(r["banded.total"])) for r in resp.data]
        assert got == [
            (1, pytest.approx(150.0)),  # 40 + 50 + 60
            (0, pytest.approx(60.0)),
        ]

    async def test_filter_on_band_in_downstream_stage(self, exec_engine) -> None:
        resp = await exec_engine.execute(_stages(
            dimensions=["region"],
            filters=["band == 1"],
            measures=[ModelMeasure(formula="bt:sum", name="total")],
        ))
        assert resp.columns == ["banded.region", "banded.total"]
        got = {r["banded.region"]: float(r["banded.total"]) for r in resp.data}
        assert got == {
            "North": pytest.approx(40.0),
            "South": pytest.approx(50.0),
            None: pytest.approx(60.0),
        }


class TestNoInternalNamesLeak:
    async def test_response_surface_is_placeholder_free(self, exec_engine) -> None:
        resp = await exec_engine.execute(_stages(
            dimensions=["band"],
            measures=[ModelMeasure(formula="bt:sum", name="total")],
        ))
        assert all("__regroup__" not in c for c in resp.columns)
        for row in resp.data:
            assert all("__regroup__" not in k for k in row)
        # attributes / warnings / sql included: nothing internal anywhere.
        assert "__regroup__" not in resp.model_dump_json()

    async def test_emitted_multistage_sql_is_placeholder_free(
        self, exec_engine,
    ) -> None:
        resp = await exec_engine.execute(_stages(
            dimensions=["band"],
            measures=[ModelMeasure(formula="bt:sum", name="total")],
        ), dry_run=True)
        assert resp.sql is not None
        assert "__regroup__" not in resp.sql
        # The regrouped dimension crosses the boundary under its PUBLIC name.
        assert "band" in resp.sql
