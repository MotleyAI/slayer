"""DEV-1835 union-grain lift (design D9, task 3.7) — a transform-in-dimension
mixing a windowed or first/last inner aggregate with a different-grain sibling
broadcasts over the union grain, with the windowed axis entering as the query's
bucketed time dimension.

Strict-xfail until task 3.7 deletes the residual DEV-1839→1835 guard (today
these raise ``NotImplementedError`` naming the deferred combination); an
accidental lift XPASSes loudly and flips the module.

Scenario coverage map (spec: openspec …/specs/queries/computed-dimensions):
  A windowed inner aggregate contributes the time bucket   test_windowed_inner…
  First/last inner aggregate mixes with a different-grain  test_first_last_inner…
  Windowed inner … without a resolvable time dimension     test_windowed_inner_no_td…
"""

from __future__ import annotations

import pytest

from slayer.sql.scope_check import assert_scope_closed

from tests._dev1835_fixtures import (
    CITY_TOTAL,
    ModelMeasure,
    REGION_MONTH_TOTAL,
    UNION_RK_DIM,
    UNION_RK_RANK,
    UNION_WM_DIM,
    UNION_WM_RANK,
    make_exec_engine,
    month_key,
    month_td,
    q,
)

LIFT_XFAIL = pytest.mark.xfail(
    strict=True, raises=NotImplementedError,
    reason="DEV-1835 task 3.7 — residual windowed/first-last union-grain guard",
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


class TestUnionGrainBroadcast:
    @LIFT_XFAIL
    async def test_windowed_inner_contributes_the_time_bucket(
        self, exec_backend,
    ) -> None:
        """Union grain (region, month bucket): the plain region total
        broadcasts across the region's buckets; the rank runs over the five
        union-grain rows (diffs -70/0/-25/0/0 → 5/1/4/1/1)."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", {"expression": UNION_WM_DIM, "name": "ur"}],
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula="amount:sum", name="m")],
        )
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], month_key(r["orders.ordered_at"])):
                (int(r["orders.ur"]), float(r["orders.m"]))
            for r in resp.data
        }
        assert set(got) == set(REGION_MONTH_TOTAL)
        assert len(resp.data) == len(REGION_MONTH_TOTAL)
        for key, (ur, m) in got.items():
            assert ur == UNION_WM_RANK[key], f"{key}"
            assert m == pytest.approx(REGION_MONTH_TOTAL[key]), f"{key}"
        dry = await engine.execute(query, dry_run=True)
        assert "__regroup__" not in dry.sql
        assert_scope_closed(dry.sql, dialect=dialect)

    @LIFT_XFAIL
    async def test_first_last_inner_with_different_grain_sibling(
        self, exec_backend,
    ) -> None:
        """Union grain (region, city): the region last broadcasts per region,
        the city total per city (diffs 0/-10/0/-25/0 → 1/4/1/5/1)."""
        dialect, engine = exec_backend
        query = q(
            dimensions=["region", "city", {"expression": UNION_RK_DIM, "name": "ur"}],
            measures=[ModelMeasure(formula="amount:sum", name="m")],
        )
        resp = await engine.execute(query)
        got = {
            (r["orders.region"], r["orders.city"]):
                (int(r["orders.ur"]), float(r["orders.m"]))
            for r in resp.data
        }
        assert set(got) == set(CITY_TOTAL)
        assert len(resp.data) == len(CITY_TOTAL)
        for key, (ur, m) in got.items():
            assert ur == UNION_RK_RANK[key], f"{key}"
            assert m == pytest.approx(CITY_TOTAL[key]), f"{key}"
        dry = await engine.execute(query, dry_run=True)
        assert "__regroup__" not in dry.sql
        assert_scope_closed(dry.sql, dialect=dialect)

    @LIFT_XFAIL
    async def test_windowed_inner_no_td_fails_with_time_resolution_error(
        self, exec_backend,
    ) -> None:
        """No resolvable time dimension → the same clear time-resolution error
        as windowed measures (today: the residual union-grain guard instead)."""
        _, engine = exec_backend
        with pytest.raises(ValueError, match="could not resolve its time dimension"):
            await engine.execute(q(
                dimensions=["region", {"expression": UNION_WM_DIM, "name": "ur"}],
                measures=[ModelMeasure(formula="amount:sum", name="m")],
            ))
