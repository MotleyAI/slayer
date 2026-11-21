"""Executed windowed-aggregation values: a trailing ``window=`` on
every aggregation, run against in-process SQLite and DuckDB and asserted against
the hand oracles in ``tests/_dev1915_fixtures.py`` (re-derived by the smoke test).

Every non-probe test fails on the current tree with the "only supported for sum
and avg" error; the two fail-closed probes assert the pre-existing input-safety
errors survive a window.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncIterator

import pytest

from slayer.core.query import OrderItem, QueryMetric, SlayerQuery, TimeDimension
from slayer.core.enums import TimeGranularity as TG
from slayer.core.models import DatasetField, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from tests import _dev1471_fixtures as D71
from tests import _dev1915_fixtures as F


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request: pytest.FixtureRequest) -> AsyncIterator[tuple[str, SlayerQueryEngine]]:
    async for engine in F.make_exec_engine(request):
        yield request.param, engine


def _approx(d):
    return {k: pytest.approx(v, rel=1e-4) for k, v in d.items()}


def _m(metric, alias):
    return QueryMetric(metric=metric, alias=alias)


# --------------------------------------------------------------------------- #
# One executed test per aggregation family, window='90d' over created_at.
# --------------------------------------------------------------------------- #
async def test_rolling_count_min_max(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("count(orders.amount, window='90d')", "c"),
        _m("min(orders.amount, window='90d')", "mn"),
        _m("max(orders.amount, window='90d')", "mx"),
    ]))
    assert {k: int(v) for k, v in F.by_month(resp, "c").items()} == F.COUNT_90D
    assert F.by_month(resp, "mn") == _approx(F.MIN_90D)
    assert F.by_month(resp, "mx") == _approx(F.MAX_90D)


async def test_rolling_count_distinct(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("count_distinct(orders.amount, window='90d')", "cd"),
    ]))
    assert {k: int(v) for k, v in F.by_month(resp, "cd").items()} == F.COUNT_DISTINCT_90D


async def test_rolling_avg_and_median(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("avg(orders.amount, window='90d')", "a"),
        _m("median(orders.amount, window='90d')", "md"),
    ]))
    assert F.by_month(resp, "a") == _approx(F.AVG_90D)
    assert F.by_month(resp, "md") == _approx(F.MEDIAN_90D)


async def test_statistics_family(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("stddev_samp(orders.amount, window='90d')", "sd"),
        _m("var_pop(orders.amount, window='90d')", "vp"),
        _m("corr(orders.amount, other=orders.qty, window='90d')", "co"),
        _m("covar_samp(orders.amount, other=orders.qty, window='90d')", "cv"),
        _m("weighted_avg(orders.amount, weight=orders.qty, window='90d')", "wa"),
    ]))
    assert F.by_month(resp, "sd") == _approx(F.STDDEV_SAMP_90D)
    assert F.by_month(resp, "vp") == _approx(F.VAR_POP_90D)
    assert F.by_month(resp, "co") == _approx(F.CORR_90D)
    assert F.by_month(resp, "cv") == _approx(F.COVAR_SAMP_90D)
    assert F.by_month(resp, "wa") == _approx(F.WEIGHTED_AVG_90D)


async def test_percentile_equals_median(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("percentile(orders.amount, p=0.5, window='90d')", "p"),
    ]))
    assert F.by_month(resp, "p") == _approx(F.MEDIAN_90D)


async def test_percentile_literal_p_rule_survives_window(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    # A window loosens no parameter rule: percentile's p must still be a literal.
    _, engine = exec_backend
    query = F.q(time_dimensions=F.month_td(),
                metrics=[_m("percentile(orders.amount, p=orders.qty, window='90d')", "p")])
    with pytest.raises(ValueError, match="must be a numeric literal"):
        await engine.execute(query)


async def test_attached_aggregate_as_windowed_weight(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("weighted_avg(orders.amount, weight=sum(orders.qty, partition_by=orders.region), window='90d')", "wa"),
    ]))
    assert F.by_month(resp, "wa") == _approx(F.WEIGHTED_AVG_ATTACHED_90D)


# --------------------------------------------------------------------------- #
# Custom aggregations.
# --------------------------------------------------------------------------- #
async def test_custom_trimmed_mean_with_and_without_overrides(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("trimmed_mean(orders.amount, lo=150, hi=350, window='90d')", "tm"),
        _m("trimmed_mean(orders.amount, window='90d')", "td"),  # defaults 0..1000 → rolling avg
    ]))
    assert F.by_month(resp, "tm") == _approx(F.TRIMMED_MEAN_150_350_90D)
    assert F.by_month(resp, "td") == _approx(F.TRIMMED_MEAN_DEFAULT_90D)


async def test_custom_on_joined_target_default_column(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        time_dimensions=F.month_td("customers.signup_at"),
        metrics=[_m("disc_spend(customers.spend, window='1y')", "ds")],
    ))
    assert F.by_month(resp, "ds") == _approx(F.DISC_SPEND_1Y)


# --------------------------------------------------------------------------- #
# first / last within the interval.
# --------------------------------------------------------------------------- #
async def test_windowed_first_last_default_axis(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("first(orders.amount, window='90d')", "f"),
        _m("last(orders.amount, window='90d')", "l"),
    ]))
    assert F.by_month(resp, "f") == _approx(F.FIRST_90D)
    assert F.by_month(resp, "l") == _approx(F.LAST_90D)


async def test_windowed_last_explicit_updated_at(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("last(orders.amount, orders.updated_at, window='90d')", "l"),
    ]))
    assert F.by_month(resp, "l") == _approx(F.LAST_UPDATED_90D)


async def test_windowed_first_updated_at_february_skips_null_key(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("first(orders.amount, orders.updated_at, window='90d')", "f"),
    ]))
    got = F.by_month(resp, "f")
    assert got["2024-01"] == pytest.approx(F.FIRST_UPDATED_JAN)
    assert got["2024-02"] == pytest.approx(F.FIRST_UPDATED_FEB)


async def test_windowed_last_all_null_key_cell(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        dimensions=["orders.region"], time_dimensions=F.month_td(),
        metrics=[_m("last(orders.amount, orders.updated_at, window='90d')", "l")],
    ))
    cell = {(r["orders.region"], F.month_key(r["orders.created_at"])): r["l"]
            for r in resp.data}
    # EU/February's interval holds only the NULL-updated_at row 3 → NULL.
    assert cell[("EU", "2024-02")] is None


# --------------------------------------------------------------------------- #
# Empty interval, partitioning, filter/order positions, cardinality.
# --------------------------------------------------------------------------- #
async def test_empty_interval_one_day(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("count(orders.amount, window='1d')", "c"),
        _m("count(orders.*, window='1d')", "sc"),
        _m("sum(orders.amount, window='1d')", "s"),
        _m("last(orders.amount, window='1d')", "l"),
    ]))
    assert set(F.by_month(resp, "c")) == set(F.EMPTY_MONTHS)  # every bucket present
    assert all(int(v) == 0 for v in F.by_month(resp, "c").values())
    assert all(int(v) == 0 for v in F.by_month(resp, "sc").values())
    assert all(v is None for v in F.by_month(resp, "s").values())
    assert all(v is None for v in F.by_month(resp, "l").values())


async def test_partitioned_count_by_region(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        dimensions=["orders.region"], time_dimensions=F.month_td(),
        metrics=[_m("count(orders.amount, window='90d', partition_by=orders.region)", "pc")],
    ))
    got = {(r["orders.region"], F.month_key(r["orders.created_at"])): int(r["pc"])
           for r in resp.data}
    assert got == F.COUNT_BY_REGION_90D


async def test_cross_model_windowed_count_and_last(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        time_dimensions=F.month_td("customers.signup_at"),
        metrics=[
            _m("count(customers.spend, window='1y')", "n"),
            _m("last(customers.spend, customers.signup_at, window='1y')", "l"),
        ],
    ))
    assert {k: int(v) for k, v in F.by_month(resp, "n").items()} == F.CM_COUNT_1Y
    assert F.by_month(resp, "l") == _approx(F.CM_LAST_1Y)


async def test_filter_only_windowed_count(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        time_dimensions=F.month_td(),
        metrics=[_m("sum(orders.amount)", "s")],
        filter=["count(orders.amount, window='90d') >= 3"],
    ))
    assert set(F.by_month(resp, "s")) == set(F.FILTER_COUNT_GE3_MONTHS)


async def test_order_only_windowed_max(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        time_dimensions=F.month_td(),
        metrics=[_m("sum(orders.amount)", "s")],
        order=[OrderItem.model_validate(
            {"field": "max(orders.amount, window='90d')", "direction": "desc"})],
    ))
    order = [F.month_key(r["orders.created_at"]) for r in resp.data]
    assert order == F.ORDER_MAX_DESC_MONTHS
    # The windowed order target is not projected into the response.
    assert [c for c in resp.columns if not c.endswith("created_at")] == ["s"]


async def test_cardinality_neutrality(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    base = await engine.execute(F.q(time_dimensions=F.month_td(),
                                    metrics=[_m("sum(orders.amount)", "s")]))
    withw = await engine.execute(F.q(time_dimensions=F.month_td(), metrics=[
        _m("sum(orders.amount)", "s"), _m("count(orders.amount, window='90d')", "w"),
    ]))
    assert len(base.data) == len(withw.data)
    assert F.by_month(base, "s") == F.by_month(withw, "s")


# --------------------------------------------------------------------------- #
# Two-stage: a windowed aggregate over the stage's own month axis equals the
# dataset-backed evaluation over the same monthly rows (stage time-axis fixture).
# --------------------------------------------------------------------------- #
# Inner stage rows + query shared with the stage time-axis tests
# (``D71.MONTHLY_ROWS`` / ``D71.MONTHLY_INNER``).
def _monthly_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="monthly", sql_table="monthly", data_source="ds",
        default_time_dimension="month_ts",
        fields=[
            DatasetField(name="month_ts", expression="month_ts", datatype=D71.DataType.DATETIME),
            DatasetField(name="rev", expression="rev", datatype=D71.DataType.FLOAT),
        ],
    )


_MONTHLY_DATASET_TABLE = {
    "name": "monthly",
    "columns": [("month_ts", "TIMESTAMP"), ("rev", "DOUBLE")],
    "rows": [("2025-01-01", 100.0), ("2025-02-01", 200.0),
             ("2025-03-01", 300.0), ("2025-04-01", 400.0)],
}

_TWO_STAGE_ORACLE = {
    "count": {"2025-01": 1, "2025-02": 2, "2025-03": 2, "2025-04": 1},
    "first": {"2025-01": 100.0, "2025-02": 100.0, "2025-03": 200.0, "2025-04": 400.0},
}


@pytest.mark.parametrize("agg,oracle", [("count", _TWO_STAGE_ORACLE["count"]),
                                        ("first", _TWO_STAGE_ORACLE["first"])])
@pytest.mark.parametrize("backend", D71.BACKENDS)
async def test_two_stage_windowed_over_stage_axis(backend, agg, oracle):
    def windowed(pop: str) -> list:
        return [{"metric": f"{agg}({pop}.rev, window='60d')", "alias": "win"}]

    stage_outer = SlayerQuery.model_validate({
        "population": "s1",
        "time_dimensions": [TimeDimension(field="s1.created_at", granularity=TG.MONTH)],
        "metrics": windowed("s1"),
    })
    model_query = SlayerQuery.model_validate({
        "population": "monthly",
        "time_dimensions": [TimeDimension(field="monthly.month_ts", granularity=TG.MONTH)],
        "metrics": windowed("monthly"),
    })
    with tempfile.TemporaryDirectory() as tmp:
        stage_engine = await D71.make_engine(
            backend, base_dir=os.path.join(tmp, "store"),
            db_path=os.path.join(tmp, f"t.{backend}"),
            tables=[D71.orders_table_spec(D71.MONTHLY_ROWS)], datasets=[D71.orders_dataset()],
        )
        stage_resp = await stage_engine.execute(query=[D71.MONTHLY_INNER, stage_outer])
        model_engine = await D71.make_engine(
            backend, base_dir=os.path.join(tmp, "model_store"),
            db_path=os.path.join(tmp, f"m.{backend}"),
            tables=[_MONTHLY_DATASET_TABLE], datasets=[_monthly_dataset()],
        )
        model_resp = await model_engine.execute(model_query)
    stage = {str(r["s1.created_at"])[:7]: r["win"] for r in stage_resp.data}
    model = {str(r["monthly.month_ts"])[:7]: r["win"] for r in model_resp.data}
    assert stage == model
    coerce = int if agg == "count" else float
    assert {k: coerce(v) for k, v in stage.items()} == oracle


# --------------------------------------------------------------------------- #
# Fail-closed probes: a window never bypasses input safety (Axiom 2.4 / 2.8).
# --------------------------------------------------------------------------- #
async def test_windowed_corr_other_across_fanning_hop_homes_on_orders(
    exec_backend: tuple[str, SlayerQueryEngine],
) -> None:
    # orders determines customers.spend, so the aggregation computes on orders from either root.
    _, engine = exec_backend
    from_customers = await engine.execute(SlayerQuery(
        population="customers", time_dimensions=F.month_td("customers.signup_at"),
        metrics=[_m("corr(customers.spend, other=orders.amount, window='1y')", "m")],
    ))
    from_orders = await engine.execute(SlayerQuery(
        population="orders", time_dimensions=F.month_td("customers.signup_at"),
        metrics=[_m("corr(customers.spend, other=orders.amount, window='1y')", "m")],
    ))
    expected = F.by_month(from_orders, "m")
    assert expected["2024-02"] == pytest.approx(1.0)
    assert F.by_month(from_customers, "m") == pytest.approx(expected, nan_ok=True)


async def test_windowed_last_ranking_across_fanning_hop_fails_closed(exec_backend: tuple[str, SlayerQueryEngine]) -> None:
    _, engine = exec_backend
    query = SlayerQuery(
        population="customers", time_dimensions=F.month_td("customers.signup_at"),
        metrics=[_m("last(customers.spend, orders.created_at, window='1y')", "m")],
    )
    with pytest.raises(ValueError, match="unproven relationship hop"):
        await engine.execute(query)


async def test_windowed_star_non_count_fails_closed():
    # `*` is legal only with count, windowed or not — a non-count star must raise,
    # never silently render <agg>(1) over the interval rows.
    query = F.q(time_dimensions=F.month_td(),
                metrics=[_m("sum(orders.*, window='90d')", "w")])
    with pytest.raises(ValueError, match="not allowed with metric"):
        await F.gen(query)


async def test_windowed_star_count_rejects_stray_args():
    # `count(*)` takes no inputs but its own window= — a stray kwarg would otherwise
    # be projected and silently ignored.
    query = F.q(time_dimensions=F.month_td(),
                metrics=[_m("count(orders.*, other=orders.qty, window='90d')", "w")])
    with pytest.raises(ValueError, match="no args or kwargs"):
        await F.gen(query)


async def test_windowed_custom_def_resolves_on_source_owner():
    # `wprod` is defined only on customers; overriding its param with a root
    # (orders) field widens the home to orders while the def stays on customers.
    # The renderer must resolve the definition on the source owner, not the root.
    query = F.q(time_dimensions=F.month_td("customers.signup_at"),
                metrics=[_m("wprod(customers.spend, w=orders.amount, window='1y')", "w")])
    sql = await F.gen(query)
    assert "__regroup__" not in sql
