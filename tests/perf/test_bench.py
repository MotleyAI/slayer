"""Performance benchmarks for SLayer queries at various data scales.

Run with: poetry run pytest tests/perf/ -v --benchmark-only
Skip with: poetry run pytest --ignore=tests/perf/
"""

import asyncio

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.models import ModelMeasure
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerResponse

from .conftest import BenchEnv
from .params import QUERY_DATE_RANGE, SCALES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _execute(env: BenchEnv, loop: asyncio.AbstractEventLoop, **query_kwargs) -> SlayerResponse:
    """Execute a query on the persistent bench loop and return the response."""
    engine, _ = env
    query = SlayerQuery(source_model="orders", **query_kwargs)
    return asyncio.run_coroutine_threadsafe(engine.execute(query=query), loop).result()


MONTHLY_TD = [TimeDimension(
    dimension="created_at", granularity=TimeGranularity.MONTH,
)]

WEEKLY_TD = [TimeDimension(
    dimension="created_at", granularity=TimeGranularity.WEEK,
)]


# ---------------------------------------------------------------------------
# Query definitions — each returns kwargs for SlayerQuery(source_model="orders", ...)
# ---------------------------------------------------------------------------

QUERIES: dict[str, dict] = {
    "simple_count": dict(
        measures=[ModelMeasure(formula="count(*)")],
    ),
    "count_by_category": dict(
        measures=[ModelMeasure(formula="count(*)"), ModelMeasure(formula="sum(total_cost)")],
        dimensions=[ColumnRef(name="category")],
    ),
    "monthly_revenue": dict(
        measures=[ModelMeasure(formula="sum(total_cost)")],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "monthly_cumsum": dict(
        measures=[ModelMeasure(formula="sum(total_cost)"), ModelMeasure(formula="cumsum(sum(total_cost))", name="running")],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "monthly_change": dict(
        measures=[ModelMeasure(formula="sum(total_cost)"), ModelMeasure(formula="change(sum(total_cost))", name="chg")],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "monthly_change_pct": dict(
        measures=[ModelMeasure(formula="sum(total_cost)"), ModelMeasure(formula="change_pct(sum(total_cost))", name="pct")],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "monthly_time_shift": dict(
        measures=[
            ModelMeasure(formula="sum(total_cost)"),
            ModelMeasure(formula="time_shift(sum(total_cost), -1, 'month')", name="prev"),
        ],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "monthly_yoy": dict(
        measures=[
            ModelMeasure(formula="sum(total_cost)"),
            ModelMeasure(formula="time_shift(sum(total_cost), -1, 'year')", name="yoy"),
        ],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "rank_by_category": dict(
        measures=[
            ModelMeasure(formula="sum(total_cost)"),
            ModelMeasure(formula="rank(sum(total_cost), direction='desc')", name="rnk"),
        ],
        dimensions=[ColumnRef(name="category")],
        order=[OrderItem(column=ColumnRef(name="total_cost_sum"), direction="desc")],
    ),
    "last_function": dict(
        measures=[
            ModelMeasure(formula="sum(total_cost)"),
            ModelMeasure(formula="last(sum(total_cost))", name="latest"),
        ],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "last_agg_type": dict(
        measures=[
            ModelMeasure(formula="sum(total_cost)"),
            ModelMeasure(formula="last(latest_cost)"),
        ],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "filtered_with_transform": dict(
        measures=[ModelMeasure(formula="sum(total_cost)")],
        time_dimensions=MONTHLY_TD,
        filters=["change(sum(total_cost)) > 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "time_shift_date_range": dict(
        measures=[
            ModelMeasure(formula="sum(total_cost)"),
            ModelMeasure(formula="time_shift(sum(total_cost), -1, 'month')", name="prev"),
        ],
        time_dimensions=[TimeDimension(
            dimension="created_at",
            granularity=TimeGranularity.MONTH,
            date_range=QUERY_DATE_RANGE,
        )],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "nested_cumsum_change": dict(
        measures=[
            ModelMeasure(formula="sum(total_cost)"),
            ModelMeasure(formula="cumsum(change(sum(total_cost)))", name="cumchg"),
        ],
        time_dimensions=MONTHLY_TD,
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "weekly_lag_lead": dict(
        measures=[
            ModelMeasure(formula="count(*)"),
            ModelMeasure(formula="lag(count(*), 1)", name="prev_week"),
            ModelMeasure(formula="lead(count(*), 1)", name="next_week"),
        ],
        time_dimensions=WEEKLY_TD,
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    ),
    "having_filter": dict(
        measures=[ModelMeasure(formula="count(*)"), ModelMeasure(formula="sum(total_cost)")],
        dimensions=[ColumnRef(name="category")],
        filters=["_count > 10"],
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
