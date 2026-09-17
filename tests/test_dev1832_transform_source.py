"""DEV-1832 task 1.6 — transforms as aggregation-source constituents (D4).

A transform nested in an aggregation source is an attached constituent typed at
its inner aggregates' explicit grains (a windowed inner adds the active bucket);
an ungrained inner is the degenerate identity + warning; a row-level leaf under
the transform that is not a projected grain key is rejected at plan time; a
time-ordered constituent without its axis fails with the time-axis error. Mixed
sources admit transform constituents and joined-model row leaves.

Fails on the current tree: nested transforms in a source are rejected at parse
(``syntax._validated_agg_source``); the row-leaf / axis checkers do not yet walk
aggregation sources; the joined-model leaf hits the cross-model rejection.
"""

from __future__ import annotations

import re

import pytest
import sqlglot
from sqlglot import exp

from slayer.engine.plan import plan_query
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1832_fixtures import (
    CHANGE_PCT_SUM_BY_MONTH,
    CHANGE_SUM_BY_MONTH,
    CONSEC_SUM_BY_MONTH,
    CUMSUM_MINUS_LAST_BY_MONTH,
    FIRST_SUM_BY_MONTH,
    GRAINED_CUMSUM_BY_MONTH,
    JOINED_ROWLEAF_MIXED_BY_STATUS,
    LAG_SUM_BY_MONTH,
    LAST_SUM_BY_MONTH,
    LEAD_SUM_BY_MONTH,
    MIXED_RANK_SUM_BY_REGION,
    TIME_SHIFT_BACK_SUM_BY_MONTH,
    UNGRAINED_CUMSUM_BY_MONTH,
    ModelMeasure,
    broadcast_warnings,
    degenerate_warnings,
    dev1832_models,
    gen,
    make_exec_engine,
    month_key,
    month_td,
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
        with pytest.raises(ValueError, match="cannot consume the row-level") as ei:
            await gen(orders_q(
                measures=[ModelMeasure(formula="sum(cumsum(weight) - 1)", name="m")],
                time_dimensions=month_td()))
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
        # (check_dimension_temporal_axis → NotImplementedError), position-neutral.
        with pytest.raises(NotImplementedError, match="time axis"):
            await gen(monthly_q(
                measures=[ModelMeasure(
                    formula="sum(cumsum(amount:sum(partition_by=region)))", name="m")],
                time_dimensions=month_td()))

    async def test_first_over_mixed_keeps_expression_error(self):
        with pytest.raises(ValueError, match="not supported over an expression"):
            await gen(sales_q(
                dimensions=["region"],
                measures=[ModelMeasure(
                    formula="first(quantity * avg(unit_price, partition_by=product))",
                    name="m")]))


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
    async def test_collapse_mixed_with_row_leaf_fails_closed(self):
        with pytest.raises(ValueError, match="collapsing transform.*row-level") as ei:
            await gen(monthly_q(
                measures=[ModelMeasure(formula=f"sum(amount * last({_X}))", name="m")],
                time_dimensions=month_td()))
        assert not re.search(r"DEV-\d+", str(ei.value))

    async def test_collapse_over_ungrained_inner_in_dimension_raises_residue(self):
        # A collapsing transform over an ungrained aggregate in a computed
        # dimension is the Axiom 9 residue error — collapse does not bypass it.
        with pytest.raises(ValueError, match="partition_by"):
            await gen(monthly_q(
                dimensions=[{"expression": "last(amount:sum)", "name": "b"}],
                measures=[ModelMeasure(formula=UNGRAINED_CUMSUM, name="m")],
                time_dimensions=month_td()))


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


# --------------------------------------------------------------------------- #
# D4e deferred shapes, fail-closed (follow-up issue, task 5.8).
# --------------------------------------------------------------------------- #
class TestDeferredInnerShapes:
    async def test_windowed_inner_fails_closed(self):
        with pytest.raises(ValueError, match="time dimension"):
            await gen(monthly_q(
                measures=[ModelMeasure(
                    formula="sum(rank(amount:sum(window='90d', partition_by=region)))",
                    name="m")],
                time_dimensions=month_td()))

    async def test_cross_model_grained_inner_fails_closed(self):
        # The host bucket is not attributable from the target (probed, fails closed).
        with pytest.raises(ValueError, match="partition_by"):
            await gen(orders_q(
                measures=[ModelMeasure(formula=(
                    "sum(cumsum(customers.spend:sum("
                    "partition_by=[customers.tier, ordered_at])))"), name="m")],
                time_dimensions=month_td()))
