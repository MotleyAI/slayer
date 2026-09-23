"""Shifted producer CTE dependencies (sql.arc42.md P6, design D7) and the
identity-deduplicated nested-plan traversal (design D8)."""

from __future__ import annotations

import re

import pytest

from slayer.engine.plan import plan_query
from slayer.engine.query_engine import _iter_plans_with_producers, _walk_regroup_attaches
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.scope_check import assert_dependency_ordered_ctes, assert_scope_closed

from tests._dev1832_fixtures import (
    ColumnRef,
    ModelMeasure,
    TimeDimension,
    TimeGranularity,
    degenerate_warnings,
    dev1832_models,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    monthly_q,
)
from tests._engine_helpers import _extract_cte_body

DIALECTS = ["postgres", "sqlite", "duckdb", "mysql", "tsql", "bigquery", "snowflake"]
SHARE = "amount:sum / amount:sum(partition_by=[ordered_at])"
REGION_SHARE = "amount:sum / amount:sum(partition_by=[region])"
DEGENERATE_SHARE = "amount:sum / max(amount:sum(partition_by=[region]), partition_by=region)"


def _q(formula: str, *, date_range: bool = False):
    td = ([TimeDimension(dimension=ColumnRef(name="ordered_at"),
                         granularity=TimeGranularity.MONTH,
                         date_range=["2024-02-01", "2024-03-31"])]
          if date_range else month_td())
    return monthly_q(dimensions=["region"], time_dimensions=td,
                     measures=[ModelMeasure(formula=f"time_shift({formula}, -1)", name="t")])


def _ctes(sql: str) -> list[str]:
    return re.findall(r"\b(\w+) AS \(", sql)


def _only(names: list[str], pattern: str) -> str:
    [name] = [n for n in names if re.fullmatch(pattern, n)]
    return name


def _reaches(sql: str, src: str, dst: str) -> bool:
    """Whether CTE ``src`` reads ``dst``, directly or through other CTEs."""
    names, seen, todo = set(_ctes(sql)), set(), [src]
    while todo:
        body = _extract_cte_body(sql, re.escape(todo.pop()))
        for ref in set(re.findall(r"\b\w+\b", body)) & names - seen:
            if ref == dst:
                return True
            seen.add(ref)
            todo.append(ref)
    return False


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestShiftedProducerOrdering:
    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_over_carried_nested_producer(self, dialect) -> None:
        sql = await gen(_q(REGION_SHARE), dialect=dialect)
        names = _ctes(sql)
        carried = _only(names, r"_cm_\w*partition_by_region\w*")
        shifted = _only(names, r"shifted_\w+")
        assert names.index(carried) < names.index(shifted)
        assert _reaches(sql, shifted, carried)
        assert_dependency_ordered_ctes(sql, dialect=dialect)
        assert_scope_closed(sql, dialect=dialect)

    @pytest.mark.parametrize("dialect", DIALECTS)
    async def test_over_interned_base_producer(self, dialect) -> None:
        sql = await gen(_q(SHARE), dialect=dialect)
        names = _ctes(sql)
        month_cm = _only(names, r"_cm_\w*partition_by_ordered_at\w*")
        shifted = _only(names, r"shifted_\w+")
        assert names.index(month_cm) < names.index(shifted)
        assert _reaches(sql, shifted, month_cm)
        assert_dependency_ordered_ctes(sql, dialect=dialect)
        assert_scope_closed(sql, dialect=dialect)

    async def test_frame_mask_splits_base_and_shifted_producers(self) -> None:
        sql = await gen(_q(SHARE, date_range=True))
        month_cms = [n for n in _ctes(sql) if re.fullmatch(r"_cm_\w*partition_by_ordered_at\w*", n)]
        assert len(month_cms) == 2, sql
        assert_dependency_ordered_ctes(sql, dialect="duckdb")
        assert_scope_closed(sql, dialect="duckdb")


class TestIdentityDeduplicatedTraversal:
    def _plan(self, formula: str):
        models = dev1832_models()
        root = next(m for m in models if m.name == "monthly")
        return plan_query(query=_q(formula), bundle=ResolvedSourceBundle(
            source_model=root, referenced_models=[m for m in models if m is not root]))

    def test_carried_attach_yielded_once(self) -> None:
        pq = self._plan(REGION_SHARE)
        walked = list(_walk_regroup_attaches(pq))
        assert len(walked) == len({id(a) for a in walked})
        [carried] = [a for a in pq.regroup_attach_plans if a.attach_phase == "combined"]
        [shifted] = [a for a in pq.regroup_attach_plans if str(a.attach_phase) == "shifted"]
        assert any(a is carried for a in shifted.producer_plan.regroup_attach_plans)
        assert sum(a is carried for a in walked) == 1

    def test_carried_producer_plan_iterated_once(self) -> None:
        pq = self._plan(REGION_SHARE)
        [carried] = [a for a in pq.regroup_attach_plans if a.attach_phase == "combined"]
        plans = list(_iter_plans_with_producers([pq]))
        assert len(plans) == len({id(p) for p in plans})
        assert sum(p is carried.producer_plan for p in plans) == 1
        assert any(str(a.attach_phase) == "shifted" for a in pq.regroup_attach_plans)

    async def test_carried_warning_not_duplicated(self, exec_engine) -> None:
        resp = await exec_engine.execute(_q(DEGENERATE_SHARE))
        assert len(degenerate_warnings(resp)) == 1
        got = {(r["monthly.region"], month_key(r["monthly.ordered_at"])): r["monthly.t"]
               for r in resp.data}
        assert len(got) == len(resp.data) == 6
        expected = {("North", "2024-02"): 10 / 60, ("North", "2024-03"): 20 / 60,
                    ("South", "2024-02"): 5 / 20}
        for cell, value in got.items():
            if cell in expected:
                assert float(value) == pytest.approx(expected[cell]), cell
            else:
                assert value is None, cell
