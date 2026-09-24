"""DEV-1832 task 1.6 — transforms as aggregation-source constituents (D4).

A transform nested in an aggregation source is an attached constituent typed at
its inner aggregates' explicit grains (a windowed inner adds the active bucket);
an ungrained inner is the degenerate identity + warning; a row-level leaf under
the transform that is not a projected grain key is rejected at plan time; a
time-ordered constituent without its axis fails with the time-axis error. Mixed
sources admit transform constituents and joined-model row leaves.

All of the above is implemented; this module guards it against regression.
"""

from __future__ import annotations

import re

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import SlayerError, TimeAxisError
from slayer.core.keys import AggregateKey, TimeTruncKey, walk_value_keys
from slayer.engine.elaborate import elaborate_query
from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1832_fixtures import (
    CHANGE_PCT_SUM_BY_MONTH,
    CHANGE_SUM_BY_MONTH,
    COLLAPSE_MIXED_BY_MONTH,
    CONSEC_SUM_BY_MONTH,
    ColumnRef,
    CUMSUM_MINUS_LAST_BY_MONTH,
    EMPTY_GRAIN_MIXED_BY_MONTH,
    FIRST_SUM_BY_MONTH,
    GRAINED_CUMSUM_BY_MONTH,
    HANDWRITTEN_MIN_MIXED_BY_MONTH,
    JOINED_ROWLEAF_MIXED_BY_STATUS,
    LAG_SUM_BY_MONTH,
    LAST_SUM_BY_MONTH,
    LEAD_SUM_BY_MONTH,
    MIXED_COMBINED_BY_REGION_MONTH,
    MIXED_MULTI_BY_MONTH,
    MIXED_RANK_SUM_BY_REGION,
    TIME_SHIFT_BACK_SUM_BY_MONTH,
    UNGRAINED_CUMSUM_BY_MONTH,
    WAVG_MIN_MIXED_BY_MONTH,
    WINDOWED_INNER_BY_MONTH,
    WINDOWED_MULTI_BY_MONTH,
    XMODEL_TIER_CUMSUM_BY_MONTH,
    ModelMeasure,
    TimeDimension,
    TimeGranularity,
    broadcast_warnings,
    degenerate_warnings,
    dev1832_models,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    monthly_multi_q,
    monthly_q,
    orders_q,
    rows_by,
    sales_q,
    status_key,
)

# Source formulas under test.
GRAINED_CUMSUM = "sum(cumsum(amount:sum(partition_by=[region, ordered_at])) - 1)"
UNGRAINED_CUMSUM = "sum(cumsum(amount:sum))"
RANK_MIXED = "sum(quantity * rank(avg(unit_price, partition_by=product)))"
JOINED_ROWLEAF_MIXED = "sum(customers.discount * avg(amount, partition_by=status))"

# Collapsing / family constituents over X = the monthly per-region total.
_X = "amount:sum(partition_by=[region, ordered_at])"
LAST_X = f"sum(last({_X}))"
FIRST_X = f"sum(first({_X}))"
CUMSUM_MINUS_LAST = f"sum(cumsum({_X}) - last({_X}))"
# Two same-op transform constituents: distinct keys (sum vs max inner), same op.
TWO_CUMSUM = f"sum(cumsum({_X}) - cumsum(amount:max(partition_by=[region, ordered_at])))"

# DEV-1928 — re-aggregation constituents in a mixed row source (row leaf * re-agg).
COLLAPSE_MIXED = f"sum(amount * last({_X}))"
HANDWRITTEN_MIN = f"sum(amount * min({_X}, partition_by=region))"
# The re-aggregation inside HANDWRITTEN_MIN, on its own (the DEV-1942 collision case).
REAGG_STANDALONE = f"min({_X}, partition_by=region)"
EMPTY_GRAIN_MIXED = "sum(amount * last(amount:sum(partition_by=ordered_at)))"
WAVG_MIN_PARAM = f"weighted_avg(amount, weight=min({_X}, partition_by=region))"
MIXED_COMBINED = (f"sum(amount * min({_X}, partition_by=region)) "
                  "+ amount:sum(partition_by=region)")
WINDOWED_INNER = "sum(rank(amount:sum(window='90d', partition_by=region)))"
# Cross-model grained inner: the host-time-axis boundary (fanning hop), the legal
# to-one key, and the non-time contrast that still associates.
XMODEL_BOUNDARY = ("sum(cumsum(customers.spend:sum("
                   "partition_by=[customers.tier, ordered_at])))")
XMODEL_TOONE = "sum(cumsum(amount:sum(partition_by=[customers.tier, ordered_at])))"
XMODEL_CONTRAST = "customers.spend:sum(partition_by=status)"
# Orders-rooted mixed re-aggregation grouped by a fanning dimension (mode axis).
FAN_DIM = "customers.regions.region_events.value"
MIXED_FAN = "sum(weight * min(amount:sum(partition_by=[status, channel]), partition_by=status))"


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _month_cols(resp, name: str) -> tuple[str, str]:
    month_col = next(c for c in resp.columns if c.endswith("ordered_at")
                     or "ordered_at" in c)
    return month_col, next(c for c in resp.columns if c.endswith("." + name))


def _by_month(resp, name: str) -> dict:
    """``{YYYY-MM: value}`` for measure ``name``, NULL-valued cells dropped."""
    month_col, meas_col = _month_cols(resp, name)
    return {month_key(row[month_col]): row[meas_col] for row in resp.data
            if row[meas_col] is not None}


def _monthly_bundle() -> ResolvedSourceBundle:
    models = dev1832_models()
    monthly = next(m for m in models if m.name == "monthly")
    return ResolvedSourceBundle(
        source_model=monthly,
        referenced_models=[m for m in models if m.name != "monthly"])


def _monthly_multi_bundle() -> ResolvedSourceBundle:
    models = dev1832_models()
    mm = next(m for m in models if m.name == "monthly_multi")
    return ResolvedSourceBundle(
        source_model=mm,
        referenced_models=[m for m in models if m.name != "monthly_multi"])


# --------------------------------------------------------------------------- #
# Executed values.
# --------------------------------------------------------------------------- #
class TestGrainedTransformConstituent:
    """queries/semantics › Grained transform constituent aggregates the cells."""

    async def test_running_total_minus_one_by_month(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=GRAINED_CUMSUM, name="m")],
            time_dimensions=month_td()))
        got = _by_month(resp, "m")
        assert got == pytest.approx(GRAINED_CUMSUM_BY_MONTH)


class TestUngrainedTransformConstituent:
    """queries/semantics › Ungrained transform constituent is identity + warning."""

    async def test_identity_plus_degenerate_warning(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=UNGRAINED_CUMSUM, name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(UNGRAINED_CUMSUM_BY_MONTH)
        assert degenerate_warnings(resp), "expected a degenerate-reaggregation warning"

    async def test_scalar_call_wrapped_reaggregation_attaches_axis(self, exec_backend):
        # The cumsum inside an aggregate that is a scalar-call argument still gets
        # its time axis attached (the attachment half of the scalar-call fix); the
        # coalesce leaves the non-null values unchanged.
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(
                formula="coalesce(sum(cumsum(amount:sum)), 0)", name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(UNGRAINED_CUMSUM_BY_MONTH)


def _year_td() -> TimeDimension:
    return TimeDimension(dimension=ColumnRef(name="ordered_at"),
                         granularity=TimeGranularity.YEAR)


class TestDualGranularityQueryGrain:
    """queries/partitioned-aggregates › the query grain is every projected time
    bucket — two granularities of one column are two grain keys."""

    @pytest.mark.parametrize("tds", [
        [*month_td(), _year_td()], [_year_td(), *month_td()],
    ])
    def test_ungrained_inner_is_grained_at_both_buckets(self, tds):
        elab = elaborate_query(
            query=monthly_q(
                measures=[ModelMeasure(formula="sum(rank(amount:sum))", name="m")],
                time_dimensions=tds),
            bundle=_monthly_bundle())
        assert elab.prebound is not None
        root = elab.prebound.declared_measures[-1].bound.value_key
        inner = next(k for k in walk_value_keys(root)
                     if isinstance(k, AggregateKey) and k is not root)
        assert inner.partition_keys is not None
        grans = {k.granularity for k in inner.partition_keys.keys
                 if isinstance(k, TimeTruncKey)}
        assert grans == {"month", "year"}

    async def test_rank_over_monthly_totals_by_executed_values(self, exec_backend):
        # Cells are the (month, year) totals 15 / 35 / 30 → ranks 3 / 1 / 2; a
        # per-column grain would rank the lone yearly total (1 everywhere).
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula="sum(rank(amount:sum))", name="m")],
            time_dimensions=[*month_td(), _year_td()]))
        got = {month_key(row["monthly.ordered_at.month"]): row["monthly.m"]
               for row in resp.data}
        assert got == {"2024-01": 3, "2024-02": 1, "2024-03": 2}
        assert degenerate_warnings(resp), "expected a degenerate-reaggregation warning"


class TestMixedTransformConstituent:
    """queries/partitioned-aggregates › Transform constituent inside a mixed source."""

    async def test_rank_mixed_by_region(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(sales_q(
            dimensions=["region"], measures=[ModelMeasure(formula=RANK_MIXED, name="m")]))
        got = {k[0]: v["sales.m"] for k, v in rows_by(resp, "sales.region").items()
               if v["sales.m"] is not None}
        assert got == pytest.approx(MIXED_RANK_SUM_BY_REGION)

    async def test_joined_model_row_leaf_mixed_by_status(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(orders_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula=JOINED_ROWLEAF_MIXED, name="m")]))
        got = {k: v["orders.m"] for k, v in status_key(resp).items()
               if v["orders.m"] is not None}
        assert got == pytest.approx(JOINED_ROWLEAF_MIXED_BY_STATUS)


# --------------------------------------------------------------------------- #
# Rejections (plan-time, via the checker — not parse).
# --------------------------------------------------------------------------- #
class TestTransformSourceRejections:
    async def test_row_leaf_under_transform_rejected(self):
        query = orders_q(
            measures=[ModelMeasure(formula="sum(cumsum(weight) - 1)", name="m")],
            time_dimensions=month_td())
        with pytest.raises(ValueError, match="cannot consume the row-level") as ei:
            await gen(query)
        assert not re.search(r"DEV-\d+", str(ei.value))

    async def test_projected_grain_key_under_transform_legal(self, exec_backend):
        # sum(rank(region)) over [region]: the transform types at the query grain
        # — the degenerate identity (one non-null row per region) plus warning.
        _, engine = exec_backend
        resp = await engine.execute(sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="sum(rank(region))", name="m")]))
        got = {k[0]: v["sales.m"] for k, v in rows_by(resp, "sales.region").items()}
        assert set(got) == {"North", "South", "East", "Gap", "Void"}
        assert all(v is not None for v in got.values())
        assert degenerate_warnings(resp), "expected a degenerate-reaggregation warning"

    async def test_transform_constituent_without_time_axis_fails(self):
        # The same time-axis error a dimension-position transform raises
        # (check_dimension_temporal_axis → TimeAxisError), position-neutral.
        query = monthly_q(
            measures=[ModelMeasure(
                formula="sum(cumsum(amount:sum(partition_by=region)))", name="m")],
            time_dimensions=month_td())
        with pytest.raises(TimeAxisError, match="time axis"):
            await gen(query)

    async def test_axis_check_covers_filter_and_order_positions(self):
        # The axis check must walk filter and order constituents too, not just
        # measures (a coarser producer grain would else reach planning).
        axis_missing = "sum(cumsum(amount:sum(partition_by=region)))"
        filter_query = monthly_q(
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            filters=[f"{axis_missing} > 5"], time_dimensions=month_td())
        with pytest.raises(TimeAxisError, match="time axis"):
            await gen(filter_query)
        order_query = monthly_q(
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            order=[{"column": axis_missing, "direction": "desc"}],
            time_dimensions=month_td())
        with pytest.raises(TimeAxisError, match="time axis"):
            await gen(order_query)

    async def test_transform_in_scalar_call_aggregate_arg_needs_time_dim(self):
        # A transform inside an aggregate that is a scalar-call argument must still
        # reach the no-time-dimension guard (time attach + unresolved-time walk
        # descend into an aggregate arg).
        query = monthly_q(measures=[ModelMeasure(
            formula="coalesce(sum(cumsum(amount:sum)), 0)", name="m")])
        with pytest.raises(ValueError, match="unambiguous time dimension"):
            await gen(query)

    async def test_first_over_mixed_keeps_expression_error(self):
        query = sales_q(
            dimensions=["region"],
            measures=[ModelMeasure(
                formula="first(quantity * avg(unit_price, partition_by=product))",
                name="m")])
        with pytest.raises(ValueError, match="not supported over an expression"):
            await gen(query)


# --------------------------------------------------------------------------- #
# Plan structure — one transform-root producer inside the carrier, flat WITH.
# --------------------------------------------------------------------------- #
class TestGrainedTransformPlanStructure:
    async def test_one_flat_with_and_parses(self):
        sql = await gen(monthly_q(
            measures=[ModelMeasure(formula=GRAINED_CUMSUM, name="m")],
            time_dimensions=month_td()))
        assert len([ln for ln in sql.split("\n")
                    if ln.strip().upper().startswith("WITH ")]) <= 1, sql

    async def test_no_placeholder_leak_in_group_by(self):
        sql = await gen(monthly_q(
            measures=[ModelMeasure(formula=GRAINED_CUMSUM, name="m")],
            time_dimensions=month_td()))
        tree = sqlglot.parse_one(sql, read="duckdb")
        for group in tree.find_all(exp.Group):
            assert "__regroup__" not in group.sql(), sql
            assert "__agg" not in group.sql(), sql

    async def test_scopes_closed(self):
        sql = await gen(monthly_q(
            measures=[ModelMeasure(formula=GRAINED_CUMSUM, name="m")],
            time_dimensions=month_td()), dialect="duckdb")
        assert_scope_closed(sql, dialect="duckdb")

    def test_exactly_one_producer_for_the_transform(self):
        planned = plan_query(
            query=monthly_q(
                measures=[ModelMeasure(formula=GRAINED_CUMSUM, name="m")],
                time_dimensions=month_td()),
            bundle=_monthly_bundle())
        assert len(planned.regroup_attach_plans) == 1


# --------------------------------------------------------------------------- #
# Collapsing constituents (D4c): first/last reduce X along the axis.
# --------------------------------------------------------------------------- #
class TestCollapsingConstituent:
    async def test_last_collapses_along_the_axis(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=LAST_X, name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(LAST_SUM_BY_MONTH)
        # The collapsed value drops the month axis, so it broadcasts across months.
        assert broadcast_warnings(resp), "expected a broadcast warning on the month dim"

    async def test_first_collapses_along_the_axis(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=FIRST_X, name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(FIRST_SUM_BY_MONTH)
        assert broadcast_warnings(resp)

    async def test_preserving_minus_collapsing_no_warning(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=CUMSUM_MINUS_LAST, name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(CUMSUM_MINUS_LAST_BY_MONTH)
        assert not broadcast_warnings(resp), "cumsum keeps the axis; no broadcast"

    async def test_filter_only_collapsing_all_survive(self, exec_backend):
        # last-sum is 45 > 40 for every month, so every cell survives unchanged.
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=LAST_X, name="m")],
            filters=[f"{LAST_X} > 40"], time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(LAST_SUM_BY_MONTH)


class TestCollapsingRejections:
    async def test_collapse_over_ungrained_inner_in_dimension_raises_residue(self):
        # A collapsing transform over an ungrained aggregate in a computed
        # dimension is the Axiom 9 residue error — collapse does not bypass it.
        query = monthly_q(
            dimensions=[{"expression": "last(amount:sum)", "name": "b"}],
            measures=[ModelMeasure(formula=UNGRAINED_CUMSUM, name="m")],
            time_dimensions=month_td())
        with pytest.raises(ValueError, match="partition_by"):
            await gen(query)


class TestCollapsingPlanStructure:
    def test_one_reaggregation_producer_no_top_level_max(self):
        planned = plan_query(
            query=monthly_q(measures=[ModelMeasure(formula=LAST_X, name="m")],
                            time_dimensions=month_td()),
            bundle=_monthly_bundle())
        # The synthesized max is discovered inside the carrier sub-plan, so the
        # top-level plan carries exactly one re-aggregation attach.
        assert len(planned.regroup_attach_plans) == 1

    async def test_one_flat_with_and_scopes_closed(self):
        sql = await gen(monthly_q(measures=[ModelMeasure(formula=LAST_X, name="m")],
                                  time_dimensions=month_td()), dialect="duckdb")
        assert len([ln for ln in sql.split("\n")
                    if ln.strip().upper().startswith("WITH ")]) <= 1, sql
        assert_scope_closed(sql, dialect="duckdb")


# --------------------------------------------------------------------------- #
# Family coverage (D4a): preserving transforms over X, summed by month.
# --------------------------------------------------------------------------- #
class TestTransformFamily:
    @pytest.mark.parametrize("op, oracle", [
        (f"change({_X})", CHANGE_SUM_BY_MONTH),
        (f"change_pct({_X})", CHANGE_PCT_SUM_BY_MONTH),
        (f"time_shift({_X}, -1)", TIME_SHIFT_BACK_SUM_BY_MONTH),
        (f"lag({_X}, 1)", LAG_SUM_BY_MONTH),
        (f"lead({_X}, 1)", LEAD_SUM_BY_MONTH),
        (f"consecutive_periods({_X} > 12)", CONSEC_SUM_BY_MONTH),
    ])
    async def test_family_by_month(self, exec_backend, op, oracle):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=f"sum({op})", name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(oracle)


class TestSameOpTransformConstituents:
    """``sum(cumsum(a) - cumsum(b))`` — two same-op transform constituents once
    collided on the bare op name (DuplicateMeasureNameError). Each now gets a
    distinct producer alias, so the source compiles."""

    async def test_two_same_op_constituents_get_distinct_aliases(self):
        sql = await gen(monthly_q(
            time_dimensions=month_td(),
            measures=[ModelMeasure(formula=TWO_CUMSUM, name="m")],
        ))
        assert "cumsum_2" in sql, sql


# --------------------------------------------------------------------------- #
# DEV-1928 — re-aggregation constituents in a mixed row source execute.
# --------------------------------------------------------------------------- #
def _reagg_plan(formula: str):
    return plan_query(
        query=monthly_q(measures=[ModelMeasure(formula=formula, name="m")],
                        time_dimensions=month_td()),
        bundle=_monthly_bundle())


def _reagg_multi_plan(formula: str):
    return plan_query(
        query=monthly_multi_q(measures=[ModelMeasure(formula=formula, name="m")],
                              time_dimensions=month_td()),
        bundle=_monthly_multi_bundle())


def _region_month(resp) -> dict:
    """``{(region, YYYY-MM): value}`` over a monthly [region]+month query, NULLs dropped."""
    month_col = next(c for c in resp.columns if "ordered_at" in c)
    meas_col = next(c for c in resp.columns if c.endswith(".m"))
    return {(row["monthly.region"], month_key(row[month_col])): row[meas_col]
            for row in resp.data if row[meas_col] is not None}


class TestReaggMixedSource:
    """queries/partitioned-aggregates › a re-aggregation constituent in a mixed source
    evaluates at its own grain and broadcasts per partition onto the source's rows."""

    async def test_collapse_mixed_executes(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=COLLAPSE_MIXED, name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(COLLAPSE_MIXED_BY_MONTH)

    async def test_handwritten_min_mixed_executes(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=HANDWRITTEN_MIN, name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(HANDWRITTEN_MIN_MIXED_BY_MONTH)

    async def test_empty_grain_mixed_broadcasts_one_value(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=EMPTY_GRAIN_MIXED, name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(EMPTY_GRAIN_MIXED_BY_MONTH)

    async def test_reaggregation_as_attached_parameter(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=WAVG_MIN_PARAM, name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(WAVG_MIN_MIXED_BY_MONTH)

    async def test_mixed_reagg_combined_with_coarser_measure(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=MIXED_COMBINED, name="m")],
            time_dimensions=month_td()))
        assert len(resp.data) == 6  # (region, month) cells incl. West's NULL row
        assert _region_month(resp) == pytest.approx(MIXED_COMBINED_BY_REGION_MONTH)

    async def test_collapse_mixed_in_filter_position(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            filters=[f"{COLLAPSE_MIXED} > 400"], time_dimensions=month_td()))
        # collapse-mixed is Jan 375 / Feb 825 / Mar 900 → only Feb, Mar survive,
        # each keeping its unfiltered amount:sum (a measure-typed filter prunes only).
        assert _by_month(resp, "s") == pytest.approx({"2024-02": 35.0, "2024-03": 30.0})

    async def test_collapse_mixed_in_order_position(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            order=[{"column": COLLAPSE_MIXED, "direction": "desc"}],
            time_dimensions=month_td()))
        month_col = next(c for c in resp.columns if "ordered_at" in c)
        months = [month_key(row[month_col]) for row in resp.data]
        assert months == ["2024-03", "2024-02", "2024-01"]


class TestReaggMixedPlanStructure:
    def test_one_reaggregation_producer_at_row_phase(self):
        [attach] = _reagg_plan(HANDWRITTEN_MIN).regroup_attach_plans
        assert attach.attach_phase == "row"
        # attached on exactly its (region) grain — one join key, one grain member.
        assert len(attach.join_pairs) == 1
        assert "region" in str(attach.join_pairs[0][0]).lower()
        assert len(attach.partition_display) == 1
        assert "region" in attach.partition_display[0].lower()

    def test_empty_grain_has_zero_join_pairs(self):
        [attach] = _reagg_plan(EMPTY_GRAIN_MIXED).regroup_attach_plans
        assert attach.attach_phase == "row"
        assert attach.join_pairs == []  # scalar cross join, never an empty ON

    async def test_one_flat_with_scopes_closed_no_leak(self):
        sql = await gen(monthly_q(
            measures=[ModelMeasure(formula=HANDWRITTEN_MIN, name="m")],
            time_dimensions=month_td()), dialect="duckdb")
        assert len([ln for ln in sql.split("\n")
                    if ln.strip().upper().startswith("WITH ")]) <= 1, sql
        assert_scope_closed(sql, dialect="duckdb")
        assert "__regroup__" not in sql

    async def test_adding_mixed_reagg_is_cardinality_neutral(self, exec_backend):
        _, engine = exec_backend
        base = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula="amount:sum", name="s")],
            time_dimensions=month_td()))
        with_mixed = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula="amount:sum", name="s"),
                      ModelMeasure(formula=HANDWRITTEN_MIN, name="m")],
            time_dimensions=month_td()))
        assert len(with_mixed.data) == len(base.data)
        # identical rows and identical shared-column (s) values, not just row count.
        assert _by_month(base, "s") == pytest.approx(_by_month(with_mixed, "s"))


class TestReaggMixedModeAxis:
    """queries/partitioned-aggregates › the mode axis is not bypassed by an attached
    re-aggregation — an orders-rooted mixed source grouped by the fanning
    ``region_events`` dimension broadcasts (with a warning) or refuses under error."""

    async def test_broadcast_repeats_and_warns(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(orders_q(
            dimensions=[FAN_DIM],
            measures=[ModelMeasure(formula=MIXED_FAN, name="m")],
            to_many_handling="broadcast"))
        vals = [row["orders.m"] for row in resp.data if row["orders.m"] is not None]
        # the SAME non-null value repeats across at least two fanning cells (the
        # region_events fixture has two distinct values) — not vacuously one/none.
        assert len(vals) >= 2
        assert len(set(vals)) == 1
        assert broadcast_warnings(resp)

    async def test_error_mode_refuses_naming_the_hop(self, exec_backend):
        _, engine = exec_backend
        query = orders_q(
            dimensions=[FAN_DIM],
            measures=[ModelMeasure(formula=MIXED_FAN, name="m")],
            to_many_handling="error")
        with pytest.raises((SlayerError, ValueError)) as ei:
            await engine.execute(query)
        msg = str(ei.value)
        assert not isinstance(ei.value, NotImplementedError)
        assert "region_events" in msg  # the mode refusal, not the grain-cover assertion


class TestWindowedInnerConstituent:
    """queries/partitioned-aggregates › Windowed inner under a transform constituent
    executes — the query's active bucket reaches the nested producer."""

    async def test_executes_three_buckets_all_nonnull(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            measures=[ModelMeasure(formula=WINDOWED_INNER, name="m")],
            time_dimensions=month_td()))
        got = _by_month(resp, "m")
        assert got == pytest.approx(WINDOWED_INNER_BY_MONTH)
        assert len(got) == 3
        assert all(v is not None for v in got.values())

    def test_nested_producer_grained_by_the_bucket(self):
        [attach] = _reagg_plan(WINDOWED_INNER).regroup_attach_plans
        # No row leaf → a second-order aggregation over the operand cells (Axiom 2.4),
        # so the producer joins at COMBINED phase, like any re-aggregation — never a
        # row-grain attach (which would count base rows; see TestReaggAxiomCompliance).
        assert attach.attach_phase == "combined"
        # the exact bucket is in BOTH the producer's projected grain AND its join keys.
        join_keys = " ".join(str(vk) for vk, _slot in attach.join_pairs).lower()
        grain = " ".join(attach.partition_display).lower()
        assert "ordered_at" in join_keys, join_keys
        assert "ordered_at" in grain, grain

    async def test_one_flat_with_scopes_closed_no_leak(self):
        sql = await gen(monthly_q(
            measures=[ModelMeasure(formula=WINDOWED_INNER, name="m")],
            time_dimensions=month_td()), dialect="duckdb")
        assert len([ln for ln in sql.split("\n")
                    if ln.strip().upper().startswith("WITH ")]) <= 1, sql
        assert_scope_closed(sql, dialect="duckdb")
        assert "__regroup__" not in sql

    async def test_rank_pins_nulls_last_on_postgres(self):
        # Rank ranks NULLs last on EVERY dialect: Postgres's native DESC is NULLS
        # FIRST, so the NULL window cell would else take rank 1 and shift the rest.
        sql = await gen(monthly_q(
            measures=[ModelMeasure(formula=WINDOWED_INNER, name="m")],
            time_dimensions=month_td()), dialect="postgres")
        assert "RANK() OVER (ORDER BY" in sql, sql
        assert "DESC NULLS LAST" in sql, sql


class TestCrossModelGrainedInnerBoundary:
    """queries/partitioned-aggregates › a target-homed inner naming a host time axis is
    a permanent boundary in EVERY mode: ``ordered_at`` is an orders column reachable from
    the customers home only across the fanning customers→orders hop — a mode-invariant
    input-safety error (Axiom 8). DEV-1941 would let associate compute the value."""

    @pytest.mark.parametrize("mode", ["broadcast", "error", "associate"])
    async def test_host_time_axis_fails_closed_every_mode(self, mode):
        query = orders_q(
            measures=[ModelMeasure(formula=XMODEL_BOUNDARY, name="m")],
            time_dimensions=month_td(), to_many_handling=mode)
        with pytest.raises(ValueError, match="partition_by") as ei:
            await gen(query)
        msg = str(ei.value)
        assert "ordered_at" in msg
        assert "attributable" in msg  # names the rule
        assert "cardinality" in msg  # states an actionable remedy
        assert not re.search(r"DEV-\d+", msg)

    async def test_to_one_cross_model_partition_key_executes(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(orders_q(
            measures=[ModelMeasure(formula=XMODEL_TOONE, name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(XMODEL_TIER_CUMSUM_BY_MONTH)

    async def test_non_time_cross_model_key_still_associates(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(orders_q(
            dimensions=["status"],
            measures=[ModelMeasure(formula=XMODEL_CONTRAST, name="w")],
            to_many_handling="associate"))
        by = {k: v["orders.w"] for k, v in status_key(resp).items()}
        assert by == pytest.approx({"new": 290.0, "ok": 420.0})


class TestReaggAxiomCompliance:
    """queries/semantics › Axiom 2.4 — a source with no row leaf is a second-order
    aggregation homed on the operand dataset, so it counts the operand's CELLS; a row
    leaf homes it back on the model rows, so it counts ROWS. The monthly_multi fixture
    (two rows per (region, month) cell) tells the two apart — the monthly fixture,
    one row per cell, cannot, so the windowed pin's phase would pass either way there.

    Pins that the windowed re-aggregation joins at COMBINED phase (over cells) — never a
    row attach that would double every bucket — while the mixed form joins at ROW."""

    async def test_windowed_pure_reagg_counts_cells_not_rows(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_multi_q(
            measures=[ModelMeasure(formula=WINDOWED_INNER, name="m")],
            time_dimensions=month_td()))
        got = _by_month(resp, "m")
        assert got == pytest.approx(WINDOWED_MULTI_BY_MONTH)
        # A count over the two base rows per cell would double every bucket.
        assert got != pytest.approx(
            {m: v * 2 for m, v in WINDOWED_MULTI_BY_MONTH.items()})

    async def test_mixed_reagg_counts_rows(self, exec_backend):
        _, engine = exec_backend
        resp = await engine.execute(monthly_multi_q(
            measures=[ModelMeasure(formula=HANDWRITTEN_MIN, name="m")],
            time_dimensions=month_td()))
        assert _by_month(resp, "m") == pytest.approx(MIXED_MULTI_BY_MONTH)

    def test_pure_reagg_combined_phase_mixed_row_phase(self):
        [pure] = _reagg_multi_plan(WINDOWED_INNER).regroup_attach_plans
        assert pure.attach_phase == "combined"  # no row leaf → over operand cells
        [mixed] = _reagg_multi_plan(HANDWRITTEN_MIN).regroup_attach_plans
        assert mixed.attach_phase == "row"  # row leaf → over model rows

    async def test_reagg_used_standalone_and_mixed_executes(self, exec_backend):
        # The same re-aggregation both on its own (combined phase) and inside a mixed
        # row aggregation (row phase): one shared producer at two phases whose nested
        # carrier is emitted before every consumer on strict dialects (DEV-1942). Each
        # use keeps the value it has alone — the standalone region-min broadcast per
        # month, the mixed per-cell amount × region-min summed.
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=REAGG_STANDALONE, name="a"),
                      ModelMeasure(formula=HANDWRITTEN_MIN, name="b")],
            time_dimensions=month_td()))
        mcol = next(c for c in resp.columns if "ordered_at" in c)

        def rm(name: str) -> dict:
            return {(r["monthly.region"], month_key(r[mcol])): r[f"monthly.{name}"]
                    for r in resp.data if r[f"monthly.{name}"] is not None}

        assert rm("a") == pytest.approx(
            {("North", "2024-01"): 10.0, ("North", "2024-02"): 10.0,
             ("North", "2024-03"): 10.0, ("South", "2024-01"): 5.0,
             ("South", "2024-02"): 5.0})
        assert rm("b") == pytest.approx(
            {("North", "2024-01"): 100.0, ("North", "2024-02"): 200.0,
             ("North", "2024-03"): 300.0, ("South", "2024-01"): 25.0,
             ("South", "2024-02"): 75.0})
