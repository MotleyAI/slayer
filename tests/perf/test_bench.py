"""Performance benchmarks for SLayer queries at various data scales.

Run with: poetry run pytest tests/perf/ -v --benchmark-only
Skip with: poetry run pytest --ignore=tests/perf/
"""

import asyncio

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.query import OrderItem, QueryMetric, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerResponse

from .conftest import BenchEnv
from .params import QUERY_DATE_RANGE, SCALES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _execute(env: BenchEnv, loop: asyncio.AbstractEventLoop, **query_kwargs) -> SlayerResponse:
    """Execute a query on the persistent bench loop and return the response."""
    engine, _ = env
    query = SlayerQuery(population="orders", **query_kwargs)
    return asyncio.run_coroutine_threadsafe(engine.execute(query=query), loop).result()


MONTHLY_TD = [TimeDimension(
    field="created_at", granularity=TimeGranularity.MONTH,
)]

WEEKLY_TD = [TimeDimension(
    field="created_at", granularity=TimeGranularity.WEEK,
)]


# ---------------------------------------------------------------------------
# Query definitions — each returns kwargs for SlayerQuery(population="orders", ...)
# ---------------------------------------------------------------------------

QUERIES: dict[str, dict] = {
    "simple_count": dict(
        metrics=[QueryMetric(metric="count(*)")],
    ),
    "count_by_category": dict(
        metrics=[QueryMetric(metric="count(*)"), QueryMetric(metric="sum(total_cost)")],
        dimensions=["category"],
    ),
    "monthly_revenue": dict(
        metrics=[QueryMetric(metric="sum(total_cost)")],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "monthly_cumsum": dict(
        metrics=[QueryMetric(metric="sum(total_cost)"), QueryMetric(metric="cumsum(sum(total_cost))", alias="running")],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "monthly_change": dict(
        metrics=[QueryMetric(metric="sum(total_cost)"), QueryMetric(metric="change(sum(total_cost))", alias="chg")],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "monthly_change_pct": dict(
        metrics=[QueryMetric(metric="sum(total_cost)"), QueryMetric(metric="change_pct(sum(total_cost))", alias="pct")],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "monthly_time_shift": dict(
        metrics=[
            QueryMetric(metric="sum(total_cost)"),
            QueryMetric(metric="time_shift(sum(total_cost), -1, 'month')", alias="prev"),
        ],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "monthly_yoy": dict(
        metrics=[
            QueryMetric(metric="sum(total_cost)"),
            QueryMetric(metric="time_shift(sum(total_cost), -1, 'year')", alias="yoy"),
        ],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "rank_by_category": dict(
        metrics=[
            QueryMetric(metric="sum(total_cost)"),
            QueryMetric(metric="rank(sum(total_cost), direction='desc')", alias="rnk"),
        ],
        dimensions=["category"],
        order=[OrderItem(field="total_cost_sum", direction="desc")],
    ),
    "last_function": dict(
        metrics=[
            QueryMetric(metric="sum(total_cost)"),
            QueryMetric(metric="last(sum(total_cost))", alias="latest"),
        ],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "last_agg_type": dict(
        metrics=[
            QueryMetric(metric="sum(total_cost)"),
            QueryMetric(metric="last(latest_cost)"),
        ],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "filtered_with_transform": dict(
        metrics=[QueryMetric(metric="sum(total_cost)")],
        time_dimensions=MONTHLY_TD,
        filter=["change(sum(total_cost)) > 0"],
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "time_shift_date_range": dict(
        metrics=[
            QueryMetric(metric="sum(total_cost)"),
            QueryMetric(metric="time_shift(sum(total_cost), -1, 'month')", alias="prev"),
        ],
        time_dimensions=[TimeDimension(
            field="created_at",
            granularity=TimeGranularity.MONTH,
            date_range=QUERY_DATE_RANGE,
        )],
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "nested_cumsum_change": dict(
        metrics=[
            QueryMetric(metric="sum(total_cost)"),
            QueryMetric(metric="cumsum(change(sum(total_cost)))", alias="cumchg"),
        ],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "weekly_lag_lead": dict(
        metrics=[
            QueryMetric(metric="count(*)"),
            QueryMetric(metric="lag(count(*), 1)", alias="prev_week"),
            QueryMetric(metric="lead(count(*), 1)", alias="next_week"),
        ],
        time_dimensions=WEEKLY_TD,
        order=[OrderItem(field="created_at", direction="asc")],
    ),
    "having_filter": dict(
        metrics=[QueryMetric(metric="count(*)"), QueryMetric(metric="sum(total_cost)")],
        dimensions=["category"],
        filter=["_count > 10"],
    ),
}

QUERY_IDS = list(QUERIES.keys())


# ---------------------------------------------------------------------------
# Dynamically generate one test class per scale from SCALES
# ---------------------------------------------------------------------------

def _make_test_class(scale_name: str) -> type:
    fixture_name = f"env_{scale_name}"

    @pytest.mark.benchmark(group=scale_name)
    class _BenchClass:
        @pytest.mark.parametrize("query_name", QUERY_IDS)
        def test_query(self, benchmark, request, bench_loop, query_name: str) -> None:
            env: BenchEnv = request.getfixturevalue(fixture_name)
            result = benchmark(lambda: _execute(env, bench_loop, **QUERIES[query_name]))
            assert result.data  # a real response came back, not an un-awaited coroutine

    _BenchClass.__name__ = f"TestBench_{scale_name}"
    _BenchClass.__qualname__ = f"TestBench_{scale_name}"
    return _BenchClass


for _scale_name in SCALES:
    globals()[f"TestBench_{_scale_name}"] = _make_test_class(_scale_name)
