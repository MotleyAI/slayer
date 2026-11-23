"""Time-ordered transforms are judged at their operand grain in every position.

Spec: queries/type-errors › The time-axis violation is a type error, not an
unimplemented feature. Values derive from the ``_dev1739_fixtures`` rows
(region-month totals N Jan 30 / Feb 70 · S Jan 25 / Mar 25 · NULL Mar 60).
"""

from __future__ import annotations

import pytest

from slayer.core.errors import TimeAxisError
from slayer.core.query import OrderItem, QueryMetric, SlayerQuery
from tests._dev1835_fixtures import (
    CUMSUM_OVER_W90,
    REGION_MONTH_TOTAL,
    _assert_map,
    _by_region_month,
    make_exec_engine,
    make_shipped_exec_engine,
    month_key,
    month_td,
    q,
)
from tests._dev2015_fixtures import cg_engine, m, spine_engine, spine_query

BACKENDS = ("sqlite", "duckdb")
AMOUNT = "sum(orders.amount)"
KEEPS_AXIS = "sum(orders.amount, partition_by=[orders.region, orders.ordered_at])"
MONTH_KEYS = {("North", "2024-01"), ("North", "2024-02"), ("South", "2024-01"), ("South", "2024-03"), (None, "2024-03")}

TIME_LESS = {
    "time_shift": "time_shift(sum(orders.amount, partition_by=orders.region), -1)",
    "change": "change(sum(orders.amount, partition_by=[]))",
    "change_pct": "change_pct(sum(orders.amount, partition_by=orders.region))",
    "cumsum": "cumsum(sum(orders.amount, partition_by=orders.region))",
}
NESTED_COLLAPSE = {
    "time_shift_last": (f"time_shift(last({KEEPS_AXIS}), -1)", "last"),
    "cumsum_last": (f"cumsum(last({KEEPS_AXIS}))", "last"),
    "time_shift_first": (f"time_shift(first({KEEPS_AXIS}), -1)", "first"),
}
WINDOWED = "cumsum(sum(orders.amount, window='90d', partition_by=orders.region))"


def _metrics(*exprs: str) -> list[QueryMetric]:
    return [QueryMetric(metric=e, alias=f"x{i}") for i, e in enumerate(exprs)]


def _in_dimension(t: str) -> dict:
    return {"dimensions": ["orders.region", {"field": f"CASE WHEN {t} > 0 THEN 1 ELSE 0 END", "alias": "d"}],
            "metrics": _metrics(AMOUNT)}


POSITIONS = {
    "measure": lambda t: {"metrics": _metrics(t)},
    "filter": lambda t: {"metrics": _metrics(AMOUNT), "filter": [f"{t} > 0"]},
    "order": lambda t: {"metrics": _metrics(AMOUNT), "order": [OrderItem(field=t, direction="desc")]},
    "arithmetic": lambda t: {"metrics": _metrics(f"{t} + {AMOUNT}")},
    "scalar_call": lambda t: {"metrics": _metrics(f"coalesce({t}, 0)")},
    "rank": lambda t: {"metrics": _metrics(f"rank({t}, direction='desc')")},
    "aggregation_source": lambda t: {"metrics": _metrics(f"max({t})")},
    "aggregation_parameter": lambda t: {"metrics": _metrics(f"weighted_avg({KEEPS_AXIS}, weight={t})")},
    "dimension": _in_dimension,
}


@pytest.fixture(params=list(BACKENDS))
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


@pytest.fixture(params=list(BACKENDS))
async def shipped_engine(request):
    async for e in make_shipped_exec_engine(request):
        yield e


def _query(**kw) -> SlayerQuery:
    kw.setdefault("dimensions", ["orders.region"])
    return q(time_dimensions=month_td(), **kw)


async def _refusal(engine, query) -> TimeAxisError:
    with pytest.raises(TimeAxisError) as info:
        await engine.execute(query=query)
    assert not isinstance(info.value, NotImplementedError)
    return info.value


class TestTimeLessOperandEveryPosition:
    @pytest.mark.parametrize("position", list(POSITIONS))
    @pytest.mark.parametrize("op", list(TIME_LESS))
    async def test_refused_with_partition_by_remedy(self, engine, op, position) -> None:
        err = await _refusal(engine, _query(**POSITIONS[position](TIME_LESS[op])))
        assert "partition_by=" in (err.suggestion or "")

    @pytest.mark.parametrize("position", list(POSITIONS))
    @pytest.mark.parametrize("op", ["time_shift", "change", "change_pct"])
    async def test_same_remedy_as_cumsum(self, engine, op, position) -> None:
        err = await _refusal(engine, _query(**POSITIONS[position](TIME_LESS[op])))
        ref = await _refusal(engine, _query(**POSITIONS[position](TIME_LESS["cumsum"])))
        assert err.suggestion == ref.suggestion


class TestAxisKeepingOperand:
    async def test_shift_equals_plain_shift(self, engine) -> None:
        resp = await engine.execute(query=_query(metrics=[
            QueryMetric(metric=f"time_shift({KEEPS_AXIS}, -1)", alias="x"),
            QueryMetric(metric=f"time_shift({AMOUNT}, -1)", alias="y"),
        ]))
        expected = {k: (30.0 if k == ("North", "2024-02") else None) for k in MONTH_KEYS}
        _assert_map(_by_region_month(resp, "x"), expected)
        _assert_map(_by_region_month(resp, "y"), expected)


class TestNestedCollapseDropsAxis:
    @pytest.mark.parametrize("position", ["measure", "aggregation_source", "dimension"])
    @pytest.mark.parametrize("case", list(NESTED_COLLAPSE))
    async def test_refused_with_collapse_remedy(self, engine, case, position) -> None:
        expr, collapse = NESTED_COLLAPSE[case]
        err = await _refusal(engine, _query(**POSITIONS[position](expr)))
        assert "partition_by=" not in (err.suggestion or "")
        assert collapse in (err.suggestion or "")

    # A collapsed literal-only transform must not fall back to the query grain.
    @pytest.mark.parametrize("dimensions", [[], ["orders.region"]])
    @pytest.mark.parametrize("expr", ["time_shift(last(cumsum(1)), -1)", "cumsum(last(cumsum(1)))"])
    async def test_aggregate_free_operand_refused_at_any_grain(self, engine, expr, dimensions) -> None:
        err = await _refusal(engine, _query(dimensions=dimensions, metrics=_metrics(expr)))
        assert "last" in (err.suggestion or "")


class TestWindowedInnerCarriesAxis:
    async def test_measure_and_source(self, engine) -> None:
        resp = await engine.execute(query=_query(metrics=[
            QueryMetric(metric=WINDOWED, alias="x"), QueryMetric(metric=f"max({WINDOWED})", alias="y"),
        ]))
        _assert_map(_by_region_month(resp, "x"), CUMSUM_OVER_W90)
        _assert_map(_by_region_month(resp, "y"), CUMSUM_OVER_W90)

    async def test_dimension(self, engine) -> None:
        resp = await engine.execute(query=_query(**_in_dimension(WINDOWED)))
        assert all(v > 0 for v in CUMSUM_OVER_W90.values())
        _assert_map(_by_region_month(resp, "d"), {k: 1.0 for k in CUMSUM_OVER_W90})
        _assert_map(_by_region_month(resp, "x0"), REGION_MONTH_TOTAL)


class TestActiveAxisResolution:
    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_custom_granularity_raw_column(self, backend) -> None:
        def cg_query(inner: str) -> SlayerQuery:
            return SlayerQuery.model_validate({
                "population": "orders",
                "time_dimensions": [{"field": "orders.order_date", "granularity": "fiscal_year"}],
                "metrics": [{"metric": f"time_shift({inner}, -1)", "alias": "x"}],
            })

        async with cg_engine(backend) as e:
            resp = await e.execute(query=cg_query("sum(orders.amount, partition_by=[orders.order_date])"))
            await _refusal(e, cg_query("sum(orders.amount, partition_by=[])"))
        assert {str(r["orders.order_date"])[:7]: r["x"] for r in resp.data} == {
            "2023-04": None, "2024-04": 10.0, "2025-04": 50.0,
        }

    async def test_downstream_stage_time_dimension(self, engine) -> None:
        inner = SlayerQuery.model_validate({
            "name": "s1", "population": "orders", "dimensions": ["orders.region"],
            "time_dimensions": [{"field": "orders.ordered_at", "granularity": "month"}],
            "metrics": [{"metric": AMOUNT, "alias": "rev"}],
        })

        def outer(partition: str) -> SlayerQuery:
            return SlayerQuery.model_validate({
                "population": "s1", "dimensions": ["s1.region"],
                "time_dimensions": [{"field": "s1.ordered_at", "granularity": "month"}],
                "metrics": [{"metric": f"time_shift(sum(s1.rev, partition_by={partition}), -1)", "alias": "x"}],
            })

        resp = await engine.execute(query=[inner, outer("[s1.region, s1.ordered_at]")])
        got = {(r["s1.region"], month_key(r["s1.ordered_at"])): r["x"] for r in resp.data}
        _assert_map(got, {k: (30.0 if k == ("North", "2024-02") else None) for k in MONTH_KEYS})
        await _refusal(engine, [inner, outer("[s1.region]")])

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_spine_bucket(self, backend) -> None:
        async with spine_engine(backend) as e:
            resp = await e.execute(query=spine_query(metrics=[
                m("time_shift(sum(orders.amount, partition_by=[time_spine.timestamp]), -1)", "x"),
            ]))
            await _refusal(e, spine_query(metrics=[m("time_shift(sum(orders.amount, partition_by=[]), -1)", "x")]))
        assert {str(r["time_spine.timestamp"])[:7]: r["x"] for r in resp.data} == {
            "2025-01": None, "2025-02": 150.0, "2025-03": 70.0,
            "2025-04": None, "2025-05": None, "2025-06": None,
        }

    @staticmethod
    def _two_td_query(metric: str) -> SlayerQuery:
        return SlayerQuery.model_validate({
            "population": "orders", "dimensions": ["orders.region"],
            "time_dimensions": [
                {"field": "orders.ordered_at", "granularity": "month"},
                {"field": "orders.shipped_at", "granularity": "month"},
            ],
            "main_time_dimension": "orders.ordered_at",
            "metrics": [{"metric": metric, "alias": "x"}],
        })

    async def test_main_time_dimension_selects_axis(self, shipped_engine) -> None:
        resp = await shipped_engine.execute(query=self._two_td_query(
            "time_shift(sum(orders.amount, partition_by=[orders.region, orders.ordered_at]), -1)",
        ))
        got = {
            (r["orders.region"], month_key(r["orders.ordered_at"]), month_key(r["orders.shipped_at"])): r["x"]
            for r in resp.data
        }
        assert got == {
            ("North", "2024-01", "2024-01"): None, ("North", "2024-01", "2024-03"): None,
            ("North", "2024-02", "2024-02"): 30.0, ("South", "2024-01", "2024-03"): None,
            ("South", "2024-03", "2024-03"): None, (None, "2024-03", "2024-03"): None,
        }

    @pytest.mark.parametrize("metric", [
        "time_shift(sum(orders.amount, partition_by=[orders.region, orders.shipped_at]), -1)",
        "cumsum(sum(orders.amount, partition_by=[orders.region, orders.shipped_at]))",
    ])
    async def test_other_time_column_is_not_the_axis(self, shipped_engine, metric) -> None:
        await _refusal(shipped_engine, self._two_td_query(metric))
