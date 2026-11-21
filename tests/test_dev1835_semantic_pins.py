"""DEV-1835 semantic pins — executed-value contracts that must hold BEFORE and
AFTER the renderer-arm rewrite: the DEV-1732 frame-bound rule, the DEV-1748
explicit-ranking-time precedence, and the ranked composition shapes that
already work today (their ``_rk_`` SQL-shape suites are rewritten with the
migration; these values may not move).
"""

from __future__ import annotations

import pytest

from slayer.core.query import QueryMetric

from tests._dev1835_fixtures import (
    FIRST_BY_SHIPPED,
    LAST_BY_ORDERED,
    LAST_BY_SHIPPED,
    OK_W90,
    REGION_LAST,
    TimeDimension,
    TimeGranularity,
    make_exec_engine,
    make_shipped_exec_engine,
    month_td,
    q,
    _by_region,
    _by_region_month,
    _assert_map,
)


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


@pytest.fixture(params=["sqlite", "duckdb"])
async def shipped_backend(request):
    async for engine in make_shipped_exec_engine(request):
        yield request.param, engine


#: Visible buckets under a >= 2024-02-01 frame bound; the windowed values must
#: still reach January's rows (the DEV-1732 rule, executed).
BOUNDED_W90 = {
    ("North", "2024-02"): 100.0, ("South", "2024-03"): 50.0,
    (None, "2024-03"): 60.0,
}


class TestFrameBoundExecuted:
    async def test_explicit_bound_equals_date_range_for_windowed(
        self, exec_backend,
    ) -> None:
        _, engine = exec_backend
        w = QueryMetric(metric="sum(orders.amount, window='90d')", alias="w")
        bounded = await engine.execute(q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            filter=["orders.ordered_at >= '2024-02-01'"], metrics=[w],
        ))
        ranged = await engine.execute(q(
            dimensions=["orders.region"],
            time_dimensions=[TimeDimension(
                field="orders.ordered_at",
                granularity=TimeGranularity.MONTH,
                date_range=["2024-02-01", "2024-12-31"],
            )],
            metrics=[w],
        ))
        _assert_map(got=_by_region_month(resp=bounded, col="w"), expected=BOUNDED_W90)
        _assert_map(got=_by_region_month(resp=ranged, col="w"), expected=BOUNDED_W90)

    async def test_explicit_bound_equals_date_range_for_time_shift(
        self, exec_backend,
    ) -> None:
        """The first visible bucket's ``prev`` reaches the invisible January."""
        _, engine = exec_backend
        prev = QueryMetric(metric="time_shift(sum(orders.amount), -1)", alias="prev")
        expected = {
            ("North", "2024-02"): 30.0, ("South", "2024-03"): None,
            (None, "2024-03"): None,
        }
        bounded = await engine.execute(q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            filter=["orders.ordered_at >= '2024-02-01'"], metrics=[prev],
        ))
        ranged = await engine.execute(q(
            dimensions=["orders.region"],
            time_dimensions=[TimeDimension(
                field="orders.ordered_at",
                granularity=TimeGranularity.MONTH,
                date_range=["2024-02-01", "2024-12-31"],
            )],
            metrics=[prev],
        ))
        _assert_map(_by_region_month(bounded, "prev"), expected)
        _assert_map(_by_region_month(ranged, "prev"), expected)

    async def test_population_filter_reaches_the_window(self, exec_backend) -> None:
        """A non-frame row filter restricts the windowed population (row 6
        drops → (South, Mar) vanishes and OK totals apply)."""
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            filter=["orders.status = 'ok'"],
            metrics=[QueryMetric(metric="sum(orders.amount, window='90d')", alias="w")],
        ))
        _assert_map(_by_region_month(resp, "w"), {
            k: v for k, v in OK_W90.items() if k != ("South", "2024-03")
        })


class TestExplicitRankingTime:
    async def test_default_and_explicit_default_agree(self, shipped_backend) -> None:
        _, engine = shipped_backend
        bare = await engine.execute(q(
            dimensions=["orders.region"],
            metrics=[QueryMetric(metric="last(orders.amount)", alias="l")],
        ))
        explicit = await engine.execute(q(
            dimensions=["orders.region"],
            metrics=[QueryMetric(metric="last(orders.amount, orders.ordered_at)", alias="l")],
        ))
        _assert_map(got=_by_region(resp=bare, col="l"), expected=LAST_BY_ORDERED)
        _assert_map(got=_by_region(resp=explicit, col="l"), expected=LAST_BY_ORDERED)

    async def test_explicit_ranking_column_wins(self, shipped_backend) -> None:
        """North's latest shipment is its earliest-amount row (10 ≠ 30)."""
        _, engine = shipped_backend
        last = await engine.execute(q(
            dimensions=["orders.region"],
            metrics=[QueryMetric(metric="last(orders.amount, orders.shipped_at)", alias="l")],
        ))
        first = await engine.execute(q(
            dimensions=["orders.region"],
            metrics=[QueryMetric(metric="first(orders.amount, orders.shipped_at)", alias="f")],
        ))
        _assert_map(_by_region(last, "l"), LAST_BY_SHIPPED)
        _assert_map(_by_region(first, "f"), FIRST_BY_SHIPPED)


class TestRankedCompositionPins:
    """Ranked shapes that already compose today — values may not move."""

    async def test_ranked_in_arithmetic(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["orders.region"],
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="m"),
                QueryMetric(metric="last(orders.amount) / sum(orders.amount)", alias="x"),
            ],
        ))
        _assert_map(_by_region(resp, "x"), {"North": 0.3, "South": 0.5, None: 1.0})

    async def test_cumsum_over_ranked(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["orders.region"], time_dimensions=month_td(),
            metrics=[QueryMetric(metric="cumsum(last(orders.amount))", alias="x")],
        ))
        _assert_map(_by_region_month(resp, "x"), {
            ("North", "2024-01"): 20.0, ("North", "2024-02"): 50.0,
            ("South", "2024-01"): 25.0, ("South", "2024-03"): 50.0,
            (None, "2024-03"): 60.0,
        })

    async def test_rank_over_ranked(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["orders.region"],
            metrics=[QueryMetric(metric="rank(last(orders.amount), direction='desc')", alias="x")],
        ))
        got = {r["orders.region"]: int(r["x"]) for r in resp.data}
        # REGION_LAST desc: NULL 60 → 1, North 30 → 2, South 25 → 3.
        assert got == {None: 1, "North": 2, "South": 3}

    async def test_selected_and_filtered_ranked(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(q(
            dimensions=["orders.region"],
            filter=["last(orders.amount) > 28"],
            metrics=[
                QueryMetric(metric="sum(orders.amount)", alias="m"),
                QueryMetric(metric="last(orders.amount)", alias="l"),
            ],
        ))
        _assert_map(
            _by_region(resp, "l"),
            {k: v for k, v in REGION_LAST.items() if v > 28},
        )
