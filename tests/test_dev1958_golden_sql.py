"""Golden SQL for the shifted-producer shapes on the five golden dialects, plus the
scope-closure / dependency-order invariant on every generated statement
(queries/transforms, "Shifted evaluation is closed and dependency-ordered").

The baseline records the pre-fix SQL (and raises); the fix re-blesses every moved
key through ``ALLOWED_DELTAS`` per the procedure in the change's ``divergences.md``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from slayer.core.query import SlayerQuery
from slayer.sql.scope_check import assert_dependency_ordered_ctes, assert_scope_closed

from tests._dev1832_fixtures import dev1832_models
from tests._engine_helpers import _engine_generate, _extract_cte_body
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent / "golden" / "dev1958_sql_baseline.json"
DIALECTS = ["postgres", "sqlite", "duckdb", "tsql", "bigquery"]
ALLOWED_DELTAS: dict[str, str] = {}

_TD = {"dimension": "ordered_at", "granularity": "month"}
_TD_DR = {**_TD, "date_range": ["2024-02-01", "2024-03-31"]}
_SHARE = "amount:sum / amount:sum(partition_by=[ordered_at])"
_REGION_SHARE = "amount:sum / amount:sum(partition_by=[region])"
_EVENT = "customers.regions.region_events.value"


def _monthly(formula: str, *, dr: bool = False, dims=("region",), **kw) -> dict:
    return {"source_model": "monthly", "dimensions": list(dims),
            "time_dimensions": [_TD_DR if dr else _TD],
            "measures": [{"formula": formula, "name": "t"}], **kw}


def _orders(formula: str, *, dims, **kw) -> dict:
    return {"source_model": "orders", "dimensions": list(dims), "time_dimensions": [_TD],
            "measures": [{"formula": formula, "name": "t"}], **kw}


def _cases() -> dict:
    return {
        "leaf/share": _monthly(f"time_shift({_SHARE}, -1)"),
        "leaf/change": _monthly(f"change({_SHARE})"),
        "leaf/change_pct": _monthly(f"change_pct({_SHARE})"),
        "leaf/trivial_composite": _monthly(
            "time_shift(amount:sum(partition_by=[ordered_at]) / 2, -1)"),
        "leaf/non_time_partition": _monthly(f"time_shift({_REGION_SHARE}, -1)"),
        "leaf/reaggregation": _monthly(
            "avg(time_shift(amount:sum(partition_by=[region, ordered_at]) "
            "/ amount:sum(partition_by=[ordered_at]), -1))", dims=()),
        "leaf/ranked_composite": _monthly("time_shift(amount:last / 2, -1)"),
        "leaf/windowed_composite": _monthly("time_shift(amount:sum(window='90d') / 2, -1)"),
        "leaf/carried_degenerate": _monthly(
            "time_shift(amount:sum / max(amount:sum(partition_by=[region]), "
            "partition_by=region), -1)"),
        "frame/bare_partitioned": _monthly(
            "time_shift(amount:sum(partition_by=[ordered_at]), -1)", dr=True),
        "frame/bare_ranked": _monthly("time_shift(amount:last, -1)", dr=True),
        "frame/bare_windowed": _monthly("time_shift(amount:sum(window='90d'), -1)", dr=True),
        "frame/non_time_partition": _monthly(f"time_shift({_REGION_SHARE}, -1)", dr=True),
        "frame/mixed_conjunction": _monthly(
            "time_shift(amount:sum + amount:sum(partition_by=[ordered_at]), -1)",
            filters=["ordered_at >= '2024-02-01' and region = 'North'"]),
        "join/two_offsets": {**_monthly(""), "measures": [
            {"formula": f"time_shift({_SHARE}, -1)", "name": "t1"},
            {"formula": f"time_shift({_SHARE}, -2)", "name": "t2"}]},
        "join/unaligned_day": _monthly("time_shift(amount:sum, -1, 'day')"),
        "population/association_filter": _orders(
            f"time_shift({_SHARE}, -1)", dims=["status"], filters=[f"{_EVENT} > 40"]),
        "population/associate_dimension": _orders(
            "time_shift(amount:sum, -1)", dims=[_EVENT], to_many_handling="associate"),
        "population/tier_partition": _orders(
            "time_shift(amount:sum / amount:sum(partition_by=[customers.tier]), -1)",
            dims=["customers.tier"]),
        "population/fanning_partition": _orders(
            f"time_shift(amount:sum / amount:sum(partition_by=[{_EVENT}]), -1)",
            dims=["status"]),
        "stage/share": [
            {"name": "s1", "source_model": "monthly", "dimensions": ["region"],
             "time_dimensions": [_TD], "measures": [{"formula": "amount:sum", "name": "rev"}]},
            {"source_model": "s1", "dimensions": ["region"], "time_dimensions": [_TD_DR],
             "measures": [{"formula": "time_shift(rev:sum / rev:sum(partition_by=[ordered_at]), -1)",
                           "name": "t"}]}],
    }


#: The one shape whose raise is the contract.
MUST_RAISE = {"population/fanning_partition"}
#: Multi-stage transform SQL gaps outside this change, keyed ``case::dialect``.
KNOWN_GAPS = {
    **{f"stage/share::{d}": "nested WITH in multi-stage SQL; DEV-1878 renders stages inline"
       for d in ("postgres", "sqlite", "duckdb")},
    "stage/share::bigquery": "DEV-1961: BigQuery transform chain over a stage leaks scope",
}


def _query(case) -> SlayerQuery | list[SlayerQuery | dict]:
    if isinstance(case, list):
        return [SlayerQuery.model_validate(stage) for stage in case]
    return SlayerQuery.model_validate(case)


async def _generate_one(case, dialect: str):
    models = dev1832_models()
    root_name = case[0]["source_model"] if isinstance(case, list) else case["source_model"]
    root = next(m for m in models if m.name == root_name)
    try:
        return await _engine_generate(
            query=_query(case), model=root,
            extra_models=[m for m in models if m is not root],
            dialect=dialect, validate=False)
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=DIALECTS,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


def _case(key: str) -> str:
    return key.split("::", 1)[0]


def test_raise_contract(baseline) -> None:
    for key, value in baseline.items():
        if key in KNOWN_GAPS:
            continue
        if _case(key) in MUST_RAISE:
            assert isinstance(value, dict), f"{key} no longer raises"
        else:
            assert not isinstance(value, dict), f"{key} records a raise: {value}"


def _scope_params() -> list:
    return [
        pytest.param(c, d, marks=[pytest.mark.xfail(strict=True, reason=KNOWN_GAPS[f"{c}::{d}"])]
                     if f"{c}::{d}" in KNOWN_GAPS else [])
        for c in sorted(set(_cases()) - MUST_RAISE) for d in DIALECTS
    ]


@pytest.mark.parametrize("case_id, dialect", _scope_params())
async def test_scope_closed_and_dependency_ordered(case_id: str, dialect: str) -> None:
    sql = await _generate_one(_cases()[case_id], dialect)
    assert isinstance(sql, str), sql
    assert_dependency_ordered_ctes(sql, dialect=dialect)
    assert_scope_closed(sql, dialect=dialect)


@pytest.mark.parametrize("case_id", sorted(
    c for c in _cases() if c.startswith(("leaf/", "frame/", "join/"))))
async def test_shifted_relation_keyed_by_unshifted_bucket(case_id: str) -> None:
    """The offset lives in the join-back, never in the shifted relation (design D3)."""
    sql = await _generate_one(_cases()[case_id], "duckdb")
    assert isinstance(sql, str), sql
    for name in set(re.findall(r"\b(shifted_\w+) AS \(", sql)):
        assert "INTERVAL" not in _extract_cte_body(sql, re.escape(name)), (name, sql)
