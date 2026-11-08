"""DEV-1942 tasks 1.1 / 1.2 — a re-aggregation attached at two phases executes.

The same re-aggregation used both standalone (combined phase) and as a mixed
row-level constituent (row phase) in one query executes with the values each use has
alone, on SQLite and DuckDB, in measure / filter / order positions and beside an
unrelated multi-stage measure. The plan carries exactly two attaches sharing one
placeholder (phases row + combined), that placeholder staged BASE; the emitted
statement lists the carrier before the re-aggregation CTE on every dialect. Under a
fanning-hop population filter the constituent's producers restrict by association
(EXISTS, no fanning join), report the semi-join under the selected measure, and a
standalone re-aggregation pushes an out-of-scope (OR-mix) conjunct by semi-join too.

Red today: the shape fails closed in the checker (dual-phase cases) and the population
constituent multiplies its rows / drops the restriction silently.

Spec: openspec …/specs/queries/partitioned-aggregates — "A re-aggregation used both
standalone and as a mixed constituent"; …/specs/queries/semantics — "Population filter
… restricts a mixed re-aggregation constituent" / "Re-aggregation producers report a
dropped out-of-scope conjunct".
"""

from __future__ import annotations

import re

import pytest

from slayer.core.keys import REGROUP_LEAF_PREFIX
from slayer.engine.plan import plan_query
from slayer.ir.planned import StageKind
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_scope_closed

from tests._dev1832_fixtures import (
    ModelMeasure,
    cust_q,
    dev1832_models,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    monthly_q,
)
from tests._dev1900_fixtures import pushed_filter_infos
from tests._engine_helpers import _extract_cte_body, _join_aliases

DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]

_X = "amount:sum(partition_by=[region, ordered_at])"
REAGG_STANDALONE = f"min({_X}, partition_by=region)"      # combined phase
HANDWRITTEN_MIN = f"sum(amount * min({_X}, partition_by=region))"  # row phase

# The dual-phase re-aggregation, per (region, month): the standalone min broadcast onto
# every month cell, and the mixed per-cell amount × region-min summed.
A_BY_REGION_MONTH = {("North", "2024-01"): 10.0, ("North", "2024-02"): 10.0,
                     ("North", "2024-03"): 10.0, ("South", "2024-01"): 5.0,
                     ("South", "2024-02"): 5.0}
B_BY_REGION_MONTH = {("North", "2024-01"): 100.0, ("North", "2024-02"): 200.0,
                     ("North", "2024-03"): 300.0, ("South", "2024-01"): 25.0,
                     ("South", "2024-02"): 75.0}

# Population fixture (customers): the ok-order population is c1/c2/c3/c5/c6; gold spend
# 190 (100+60+30), silver 230 (150+80); avg(spend:sum(pb=[tier, plan_code]), pb=tier) =
# gold 95 ((160+30)/2), silver 115 ((150+80)/2); so sum(spend*avg) = gold 18050, silver
# 26450 — never the fan-defect 27550 (c1's two ok orders double-counted).
_OK = "orders.status = 'ok'"
_OR_MIX = "tier = 'bronze' or orders.status = 'ok'"
POP_MIXED = "sum(spend * avg(spend:sum(partition_by=[tier, plan_code]), partition_by=tier))"
POP_AVG = "avg(spend:sum(partition_by=[tier, plan_code]), partition_by=tier)"
POP_MIN = "min(spend:sum(partition_by=[tier, plan_code]), partition_by=tier)"
POP_MIXED_BY_TIER = {"gold": 18050.0, "silver": 26450.0}
POP_AVG_BY_TIER = {"gold": 95.0, "silver": 115.0}
# Under the OR-mix the population is c1/c2/c3/c5/c6 (an ok order) + c4 (bronze); c7
# (gold, no orders) is out. Per-(tier, plan) spend: gold p1 160 / NULL 30, silver p2
# 150 / p3 80, bronze p2 40 → min gold 30, silver 80, bronze 40.
POP_MIN_OR_MIX_BY_TIER = {"gold": 30.0, "silver": 80.0, "bronze": 40.0}


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_backend(request):
    async for engine in make_exec_engine(request):
        yield request.param, engine


def _region_month(resp, name: str) -> dict:
    """``{(region, YYYY-MM): value}`` for measure ``name``, NULL-valued cells dropped."""
    mcol = next(c for c in resp.columns if "ordered_at" in c)
    vcol = next(c for c in resp.columns if c.endswith("." + name))
    return {(r["monthly.region"], month_key(r[mcol])): r[vcol]
            for r in resp.data if r[vcol] is not None}


def _by_tier(resp, name: str) -> dict:
    return {r["customers.tier"]: r["customers." + name] for r in resp.data
            if r["customers." + name] is not None}


def _cm_ctes(sql: str) -> list[str]:
    return re.findall(r"(_cm_\w+) AS \(", sql)


def _monthly_bundle() -> ResolvedSourceBundle:
    models = dev1832_models()
    monthly = next(m for m in models if m.name == "monthly")
    return ResolvedSourceBundle(
        dialect="duckdb",
        source_model=monthly,
        referenced_models=[m for m in models if m.name != "monthly"])


def _dual_phase_query():
    return monthly_q(
        dimensions=["region"],
        measures=[ModelMeasure(formula=REAGG_STANDALONE, name="a"),
                  ModelMeasure(formula=HANDWRITTEN_MIN, name="b")],
        time_dimensions=month_td())


# --------------------------------------------------------------------------- #
# Execution — the dual-phase shape (monthly).
# --------------------------------------------------------------------------- #
class TestDualPhaseExecution:
    async def test_measure_positions_execute(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(_dual_phase_query())
        assert _region_month(resp, "a") == pytest.approx(A_BY_REGION_MONTH)
        assert _region_month(resp, "b") == pytest.approx(B_BY_REGION_MONTH)

    async def test_standalone_in_filter_beside_mixed(self, exec_backend) -> None:
        # min>5 keeps North (10), prunes South (5) and West (NULL) — a measure-typed mask.
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=HANDWRITTEN_MIN, name="b")],
            filters=[f"{REAGG_STANDALONE} > 5"],
            time_dimensions=month_td()))
        assert {r["monthly.region"] for r in resp.data} == {"North"}
        assert _region_month(resp, "b") == pytest.approx(
            {k: v for k, v in B_BY_REGION_MONTH.items() if k[0] == "North"})

    async def test_standalone_in_order_beside_mixed(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(monthly_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=REAGG_STANDALONE, name="a"),
                      ModelMeasure(formula=HANDWRITTEN_MIN, name="b")],
            order=[{"column": REAGG_STANDALONE, "direction": "desc"}],
            time_dimensions=month_td()))
        regions = [r["monthly.region"] for r in resp.data]
        north = [i for i, r in enumerate(regions) if r == "North"]
        south = [i for i, r in enumerate(regions) if r == "South"]
        assert max(north) < min(south)  # North (min 10) sorts before South (min 5)

    async def test_shape_with_unrelated_cumsum_executes(self, exec_backend) -> None:
        # The multi-stage steps prelude (cumsum) beside the dual-phase shape; adding the
        # dual-phase measures leaves the cumsum's own values unchanged.
        _, engine = exec_backend
        alone = await engine.execute(monthly_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula="cumsum(amount:sum)", name="c")],
            time_dimensions=month_td()))
        resp = await engine.execute(monthly_q(
            dimensions=["region"],
            measures=[ModelMeasure(formula=REAGG_STANDALONE, name="a"),
                      ModelMeasure(formula=HANDWRITTEN_MIN, name="b"),
                      ModelMeasure(formula="cumsum(amount:sum)", name="c")],
            time_dimensions=month_td()))
        assert _region_month(resp, "a") == pytest.approx(A_BY_REGION_MONTH)
        assert _region_month(resp, "b") == pytest.approx(B_BY_REGION_MONTH)
        assert _region_month(resp, "c") == pytest.approx(_region_month(alone, "c"))


# --------------------------------------------------------------------------- #
# Plan — two attaches, one placeholder, phases {row, combined}, BASE staging.
# --------------------------------------------------------------------------- #
class TestDualPhasePlan:
    def test_two_attaches_share_one_placeholder_at_two_phases(self) -> None:
        plan = plan_query(query=_dual_phase_query(), bundle=_monthly_bundle())
        attaches = plan.regroup_attach_plans
        assert len(attaches) == 2
        assert {a.attach_phase for a in attaches} == {"row", "combined"}
        placeholders = {sub.placeholder for a in attaches for sub in a.substitutions}
        assert len(placeholders) == 1

    def test_shared_placeholder_slot_is_staged_base(self) -> None:
        plan = plan_query(query=_dual_phase_query(), bundle=_monthly_bundle())
        [ph] = {sub.placeholder for a in plan.regroup_attach_plans
                for sub in a.substitutions}
        top = (list(plan.row_slots) + list(plan.aggregate_slots)
               + list(plan.combined_expression_slots))
        ph_slots = [s for s in top if s.key == ph]
        assert ph_slots
        assert all(s.stage is not None and s.stage.kind == StageKind.BASE
                   for s in ph_slots)

    async def test_no_placeholder_leaks_into_sql(self) -> None:
        sql = await gen(_dual_phase_query())
        assert REGROUP_LEAF_PREFIX not in sql


# --------------------------------------------------------------------------- #
# Emission — exactly two `_cm_` CTEs, carrier first, scope-closed, per dialect.
# --------------------------------------------------------------------------- #
class TestDualPhaseEmission:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_carrier_before_reaggregation_and_scope_closed(self, dialect) -> None:
        sql = await gen(_dual_phase_query(), dialect=dialect)
        ctes = _cm_ctes(sql)
        assert len(ctes) == 2, ctes
        [carrier] = [c for c in ctes if "min" not in c]
        [reagg] = [c for c in ctes if "min" in c]
        assert ctes.index(carrier) < ctes.index(reagg)  # nested producer first
        assert_scope_closed(sql, dialect=dialect)


# --------------------------------------------------------------------------- #
# Population — the fanning-hop filter restricts the mixed constituent.
# --------------------------------------------------------------------------- #
class TestPopulationRestriction:
    async def test_mixed_constituent_restricted_by_association(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"],
            measures=[ModelMeasure(formula=POP_MIXED, name="m")],
            filters=[_OK]))
        got = _by_tier(resp, "m")
        assert got == pytest.approx(POP_MIXED_BY_TIER)
        assert got["gold"] != pytest.approx(27550.0)  # never the fan-multiplied total
        named = {i.measure for i in pushed_filter_infos(resp) if i.measure is not None}
        assert named == {"m"}

    async def test_mixed_constituent_sql_has_exists_and_no_orders_join(self) -> None:
        sql = await gen(cust_q(
            dimensions=["tier"],
            measures=[ModelMeasure(formula=POP_MIXED, name="m")],
            filters=[_OK]), dialect="duckdb")
        # Every producer carries the population semi-join; none joins orders.
        for name in _cm_ctes(sql):
            assert "EXISTS" in _extract_cte_body(sql, re.escape(name)), name
        assert "orders" not in _join_aliases(sql, dialect="duckdb")

    async def test_standalone_avg_beside_mixed_shares_one_producer(self, exec_backend) -> None:
        _, engine = exec_backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"],
            measures=[ModelMeasure(formula=POP_AVG, name="a"),
                      ModelMeasure(formula=POP_MIXED, name="m")],
            filters=[_OK]))
        assert _by_tier(resp, "a") == pytest.approx(POP_AVG_BY_TIER)
        assert _by_tier(resp, "m") == pytest.approx(POP_MIXED_BY_TIER)
        named = {i.measure for i in pushed_filter_infos(resp) if i.measure is not None}
        assert named == {"a", "m"}
        sql = await gen(cust_q(
            dimensions=["tier"],
            measures=[ModelMeasure(formula=POP_AVG, name="a"),
                      ModelMeasure(formula=POP_MIXED, name="m")],
            filters=[_OK]), dialect="duckdb")
        assert len([c for c in _cm_ctes(sql) if "avg" in c]) == 1  # avg producer, once

    async def test_standalone_reaggregation_pushes_out_of_scope_conjunct(self, exec_backend) -> None:
        # An OR-mix out-of-scope conjunct restricts the re-aggregation's producers by
        # semi-join, exactly as a plain partitioned producer — never a silent drop.
        _, engine = exec_backend
        resp = await engine.execute(cust_q(
            dimensions=["tier"],
            measures=[ModelMeasure(formula=POP_MIN, name="mn")],
            filters=[_OR_MIX]))
        assert _by_tier(resp, "mn") == pytest.approx(POP_MIN_OR_MIX_BY_TIER)
        pushed = pushed_filter_infos(resp)
        assert {i.measure for i in pushed if i.measure is not None} == {"mn"}
        # The entries name the full out-of-scope conjunct — both legs of the OR.
        assert all("status" in i.filter_text and "bronze" in i.filter_text
                   for i in pushed)

    async def test_standalone_reaggregation_sql_carries_exists_in_every_producer(self) -> None:
        # The pushed conjunct lands in the carrier AND the outer producer (a dropped
        # conjunct would leave a producer without its EXISTS).
        sql = await gen(cust_q(
            dimensions=["tier"],
            measures=[ModelMeasure(formula=POP_MIN, name="mn")],
            filters=[_OR_MIX]), dialect="duckdb")
        ctes = _cm_ctes(sql)
        assert len(ctes) == 2, ctes
        for name in ctes:
            assert "EXISTS" in _extract_cte_body(sql, re.escape(name)), name
