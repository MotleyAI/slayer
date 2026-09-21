"""Executed windowed-aggregation values (DEV-1915): a trailing ``window=`` on
every aggregation, run against in-process SQLite and DuckDB and asserted against
the hand oracles in ``tests/_dev1915_fixtures.py`` (re-derived by the smoke test).

Every non-probe test fails on the current tree with the "only supported for sum
and avg" error; the two fail-closed probes assert the pre-existing input-safety
errors survive a window.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from slayer.core.query import OrderItem, SlayerQuery, TimeDimension, ColumnRef
from slayer.core.enums import TimeGranularity as TG
from slayer.core.models import Column, ModelMeasure, SlayerModel
from tests import _dev1471_fixtures as D71
from tests import _dev1915_fixtures as F


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in F.make_exec_engine(request):
        yield request.param, engine


def _approx(d):
    return {k: pytest.approx(v, rel=1e-4) for k, v in d.items()}


def _m(formula, name):
    return ModelMeasure(formula=formula, name=name)


# --------------------------------------------------------------------------- #
# One executed test per aggregation family, window='90d' over created_at.
# --------------------------------------------------------------------------- #
async def test_rolling_count_min_max(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:count(window='90d')", "c"),
        _m("amount:min(window='90d')", "mn"),
        _m("amount:max(window='90d')", "mx"),
    ]))
    assert {k: int(v) for k, v in F.by_month(resp, "orders.c").items()} == F.COUNT_90D
    assert F.by_month(resp, "orders.mn") == _approx(F.MIN_90D)
    assert F.by_month(resp, "orders.mx") == _approx(F.MAX_90D)


async def test_rolling_count_distinct(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:count_distinct(window='90d')", "cd"),
    ]))
    assert {k: int(v) for k, v in F.by_month(resp, "orders.cd").items()} == F.COUNT_DISTINCT_90D


async def test_rolling_avg_and_median(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:avg(window='90d')", "a"),
        _m("amount:median(window='90d')", "md"),
    ]))
    assert F.by_month(resp, "orders.a") == _approx(F.AVG_90D)
    assert F.by_month(resp, "orders.md") == _approx(F.MEDIAN_90D)


async def test_statistics_family(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:stddev_samp(window='90d')", "sd"),
        _m("amount:var_pop(window='90d')", "vp"),
        _m("amount:corr(other=qty, window='90d')", "co"),
        _m("amount:covar_samp(other=qty, window='90d')", "cv"),
        _m("amount:weighted_avg(weight=qty, window='90d')", "wa"),
    ]))
    assert F.by_month(resp, "orders.sd") == _approx(F.STDDEV_SAMP_90D)
    assert F.by_month(resp, "orders.vp") == _approx(F.VAR_POP_90D)
    assert F.by_month(resp, "orders.co") == _approx(F.CORR_90D)
    assert F.by_month(resp, "orders.cv") == _approx(F.COVAR_SAMP_90D)
    assert F.by_month(resp, "orders.wa") == _approx(F.WEIGHTED_AVG_90D)


async def test_percentile_equals_median(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:percentile(p=0.5, window='90d')", "p"),
    ]))
    assert F.by_month(resp, "orders.p") == _approx(F.MEDIAN_90D)


async def test_percentile_literal_p_rule_survives_window(exec_backend):
    # A window loosens no parameter rule: percentile's p must still be a literal.
    _, engine = exec_backend
    query = F.q(time_dimensions=F.month_td(),
                measures=[_m("amount:percentile(p=qty, window='90d')", "p")])
    with pytest.raises(ValueError, match="must be a numeric literal"):
        await engine.execute(query)


async def test_attached_aggregate_as_windowed_weight(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:weighted_avg(weight=qty:sum(partition_by=region), window='90d')", "wa"),
    ]))
    assert F.by_month(resp, "orders.wa") == _approx(F.WEIGHTED_AVG_ATTACHED_90D)


# --------------------------------------------------------------------------- #
# Custom aggregations.
# --------------------------------------------------------------------------- #
async def test_custom_trimmed_mean_with_and_without_overrides(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:trimmed_mean(lo=150, hi=350, window='90d')", "tm"),
        _m("amount:trimmed_mean(window='90d')", "td"),  # defaults 0..1000 → rolling avg
    ]))
    assert F.by_month(resp, "orders.tm") == _approx(F.TRIMMED_MEAN_150_350_90D)
    assert F.by_month(resp, "orders.td") == _approx(F.TRIMMED_MEAN_DEFAULT_90D)


async def test_custom_on_joined_target_default_column(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        time_dimensions=F.month_td("customers.signup_at"),
        measures=[_m("customers.spend:disc_spend(window='1y')", "ds")],
    ))
    assert F.by_month(resp, "orders.ds") == _approx(F.DISC_SPEND_1Y)


# --------------------------------------------------------------------------- #
# first / last within the interval.
# --------------------------------------------------------------------------- #
async def test_windowed_first_last_default_axis(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:first(window='90d')", "f"),
        _m("amount:last(window='90d')", "l"),
    ]))
    assert F.by_month(resp, "orders.f") == _approx(F.FIRST_90D)
    assert F.by_month(resp, "orders.l") == _approx(F.LAST_90D)


async def test_windowed_last_explicit_updated_at(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:last(updated_at, window='90d')", "l"),
    ]))
    assert F.by_month(resp, "orders.l") == _approx(F.LAST_UPDATED_90D)


async def test_windowed_first_updated_at_february_native_nulls(exec_backend):
    dialect, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:first(updated_at, window='90d')", "f"),
    ]))
    got = F.by_month(resp, "orders.f")
    assert got["2024-01"] == pytest.approx(F.FIRST_UPDATED_JAN)
    assert got["2024-02"] == pytest.approx(F.FIRST_UPDATED_FEB[dialect])


async def test_windowed_last_all_null_key_cell(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        dimensions=["region"], time_dimensions=F.month_td(),
        measures=[_m("amount:last(updated_at, window='90d')", "l")],
    ))
    cell = {(r["orders.region"], F.month_key(r["orders.created_at"])): r["orders.l"]
            for r in resp.data}
    # EU/February's interval holds only the NULL-updated_at row 3 → still picked.
    assert cell[("EU", "2024-02")] == pytest.approx(300.0)


# --------------------------------------------------------------------------- #
# Empty interval, partitioning, filter/order positions, cardinality.
# --------------------------------------------------------------------------- #
async def test_empty_interval_one_day(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:count(window='1d')", "c"),
        _m("*:count(window='1d')", "sc"),
        _m("amount:sum(window='1d')", "s"),
        _m("amount:last(window='1d')", "l"),
    ]))
    assert set(F.by_month(resp, "orders.c")) == set(F.EMPTY_MONTHS)  # every bucket present
    assert all(int(v) == 0 for v in F.by_month(resp, "orders.c").values())
    assert all(int(v) == 0 for v in F.by_month(resp, "orders.sc").values())
    assert all(v is None for v in F.by_month(resp, "orders.s").values())
    assert all(v is None for v in F.by_month(resp, "orders.l").values())


async def test_partitioned_count_by_region(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        dimensions=["region"], time_dimensions=F.month_td(),
        measures=[_m("amount:count(window='90d', partition_by=region)", "pc")],
    ))
    got = {(r["orders.region"], F.month_key(r["orders.created_at"])): int(r["orders.pc"])
           for r in resp.data}
    assert got == F.COUNT_BY_REGION_90D


async def test_cross_model_windowed_count_and_last(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        time_dimensions=F.month_td("customers.signup_at"),
        measures=[
            _m("customers.spend:count(window='1y')", "n"),
            _m("customers.spend:last(customers.signup_at, window='1y')", "l"),
        ],
    ))
    assert {k: int(v) for k, v in F.by_month(resp, "orders.n").items()} == F.CM_COUNT_1Y
    assert F.by_month(resp, "orders.l") == _approx(F.CM_LAST_1Y)


async def test_filter_only_windowed_count(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        time_dimensions=F.month_td(),
        measures=[_m("amount:sum", "s")],
        filters=["amount:count(window='90d') >= 3"],
    ))
    assert set(F.by_month(resp, "orders.s")) == set(F.FILTER_COUNT_GE3_MONTHS)


async def test_order_only_windowed_max(exec_backend):
    _, engine = exec_backend
    resp = await engine.execute(F.q(
        time_dimensions=F.month_td(),
        measures=[_m("amount:sum", "s")],
        order=[OrderItem.model_validate(
            {"column": "amount:max(window='90d')", "direction": "desc"})],
    ))
    order = [F.month_key(r["orders.created_at"]) for r in resp.data]
    assert order == F.ORDER_MAX_DESC_MONTHS
    # The windowed order target is not projected into the response.
    assert [c for c in resp.columns if not c.endswith("created_at")] == ["orders.s"]


async def test_cardinality_neutrality(exec_backend):
    _, engine = exec_backend
    base = await engine.execute(F.q(time_dimensions=F.month_td(),
                                    measures=[_m("amount:sum", "s")]))
    withw = await engine.execute(F.q(time_dimensions=F.month_td(), measures=[
        _m("amount:sum", "s"), _m("amount:count(window='90d')", "w"),
    ]))
    assert len(base.data) == len(withw.data)
    assert F.by_month(base, "orders.s") == F.by_month(withw, "orders.s")


# --------------------------------------------------------------------------- #
# Two-stage: a windowed aggregate over the stage's own month axis equals the
# model-backed evaluation over the same monthly rows (DEV-1471 stage fixture).
# --------------------------------------------------------------------------- #
# Inner stage rows + query shared with the DEV-1471 stage-axis tests
# (``D71.MONTHLY_ROWS`` / ``D71.MONTHLY_INNER``).
def _monthly_model() -> SlayerModel:
    return SlayerModel(
        name="monthly", sql_table="monthly", data_source="ds",
        default_time_dimension="month_ts",
        columns=[
            Column(name="month_ts", sql="month_ts", type=D71.DataType.TIMESTAMP),
            Column(name="rev", sql="rev", type=D71.DataType.DOUBLE),
        ],
    )


_MONTHLY_MODEL_TABLE = {
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
    windowed = [{"formula": f"rev:{agg}(window='60d')", "name": "win"}]
    stage_outer = SlayerQuery.model_validate({
        "source_model": "s1",
        "time_dimensions": [TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TG.MONTH)],
        "measures": windowed,
    })
    model_query = SlayerQuery.model_validate({
        "source_model": "monthly",
        "time_dimensions": [TimeDimension(dimension=ColumnRef(name="month_ts"), granularity=TG.MONTH)],
        "measures": windowed,
    })
    with tempfile.TemporaryDirectory() as tmp:
        stage_engine = await D71.make_engine(
            backend, base_dir=os.path.join(tmp, "store"),
            db_path=os.path.join(tmp, f"t.{backend}"),
            tables=[D71.orders_table_spec(D71.MONTHLY_ROWS)], models=[D71.orders_model()],
        )
        stage_resp = await stage_engine.execute(query=[D71.MONTHLY_INNER, stage_outer])
        model_engine = await D71.make_engine(
            backend, base_dir=os.path.join(tmp, "model_store"),
            db_path=os.path.join(tmp, f"m.{backend}"),
            tables=[_MONTHLY_MODEL_TABLE], models=[_monthly_model()],
        )
        model_resp = await model_engine.execute(model_query)
    stage = {str(r["s1.created_at"])[:7]: r["s1.win"] for r in stage_resp.data}
    model = {str(r["monthly.month_ts"])[:7]: r["monthly.win"] for r in model_resp.data}
    assert stage == model
    coerce = int if agg == "count" else float
    assert {k: coerce(v) for k, v in stage.items()} == oracle


# --------------------------------------------------------------------------- #
# Fail-closed probes: a window never bypasses input safety (Axiom 2.4 / 2.8).
# --------------------------------------------------------------------------- #
async def test_windowed_corr_other_across_fanning_hop_fails_closed(exec_backend):
    _, engine = exec_backend
    query = SlayerQuery(
        source_model="customers", time_dimensions=F.month_td("signup_at"),
        measures=[_m("spend:corr(other=orders.amount, window='1y')", "m")],
    )
    with pytest.raises(ValueError, match="unproven join hop"):
        await engine.execute(query)


async def test_windowed_last_ranking_across_fanning_hop_fails_closed(exec_backend):
    _, engine = exec_backend
    query = SlayerQuery(
        source_model="customers", time_dimensions=F.month_td("signup_at"),
        measures=[_m("spend:last(orders.created_at, window='1y')", "m")],
    )
    with pytest.raises(ValueError, match="unproven join hop"):
        await engine.execute(query)


async def test_windowed_star_non_count_fails_closed():
    # `*` is legal only with count, windowed or not — a non-count star must raise,
    # never silently render <agg>(1) over the interval rows.
    query = F.q(time_dimensions=F.month_td(),
                measures=[_m("*:sum(window='90d')", "w")])
    with pytest.raises(ValueError, match="not allowed with measure"):
        await F.gen(query)


async def test_windowed_star_count_rejects_stray_args():
    # `*:count` takes no inputs but its own window= — a stray kwarg would otherwise
    # be projected and silently ignored.
    query = F.q(time_dimensions=F.month_td(),
                measures=[_m("*:count(other=qty, window='90d')", "w")])
    with pytest.raises(ValueError, match="no args or kwargs"):
        await F.gen(query)


async def test_windowed_custom_def_resolves_on_source_owner():
    # `wprod` is defined only on customers; overriding its param with a root
    # (orders) column widens the home to orders while the def stays on customers.
    # The renderer must resolve the definition on the source owner, not the root.
    query = F.q(time_dimensions=F.month_td("customers.signup_at"),
                measures=[_m("customers.spend:wprod(w=amount, window='1y')", "w")])
    sql = await F.gen(query)
    assert "__regroup__" not in sql
