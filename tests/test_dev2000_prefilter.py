"""Converted time bounds keep a raw-column pre-filter widened by two days, in the stored column's type, never on
frame bounds (spec: queries/time-zones › Converted time bounds stay prunable)."""

from __future__ import annotations

import re

import pytest

from tests._dev2000_emission_fixtures import TIER1, parse, raw_sides, render, zoned_query
from tests._dev2000_fixtures import BACKENDS, NY, local, m, q, tz_engine

LOW, HIGH = "2024-12-30 00:00:00", "2025-02-03 00:00:00"
JAN_FILTER = ["{f} >= '2025-01-01'", "{f} < '2025-02-01'"]
_ZONED_LITERAL = re.compile(r"TIMESTAMPTZ|TIMESTAMP WITH TIME ZONE|TIMESTAMP_TZ|TIMESTAMP_LTZ|\+00(:00)?'|Z'|"
                            r"DATETIMEOFFSET|'UTC'|TIMESTAMP\(", re.IGNORECASE)


def _jan(name: str) -> list[str]:
    return [f.format(f=f"orders.{name}") for f in JAN_FILTER]


def _shapes(name: str) -> dict[str, dict]:
    ref = f"orders.{name}"
    return {
        "comparison": {"dimensions": ["orders.id"], "filter": _jan(name)},
        "date_trunc": {"dimensions": ["orders.id"], "filter": [f"date_trunc('month', {ref}) = '2025-01-01'"]},
        "date_range": {"dimensions": ["orders.id"],
                       "time_dimensions": [{"field": ref, "granularity": "day", "date_range": ["2025-01-01", "2025-01-31"]}]},
    }


def _expected_ids() -> set[int]:
    return {i for i in range(1, 5) if local(i, NY).year == 2025 and local(i, NY).month == 1}


@pytest.mark.parametrize("shape", ["comparison", "date_trunc", "date_range"])
@pytest.mark.parametrize("name", ["ordered_at", "paid_at"])
@pytest.mark.parametrize("backend", BACKENDS)
async def test_widened_bounds_keep_the_exact_result(backend: str, name: str, shape: str) -> None:
    async with tz_engine(backend) as engine:
        resp = await engine.execute(q(time_zone=NY, **_shapes(name)[shape]))
    assert {int(r["orders.id"]) for r in resp.data} == _expected_ids() == {2, 3}, resp.sql
    assert resp.sql is not None
    tree = parse(resp.sql, backend)
    texts = [o.sql(dialect=backend) for _s, o in raw_sides(tree, name)]
    assert any(LOW[:10] in t for t in texts), resp.sql
    assert any(HIGH[:10] in t for t in texts), resp.sql


@pytest.mark.parametrize("dialect", TIER1)
async def test_naive_pre_filter_literal_is_naive(dialect: str) -> None:
    sql = await render(dialect, zoned_query(**_shapes("ordered_at")["comparison"]))
    tree = parse(sql, dialect)
    others = [o.sql(dialect=dialect) for _s, o in raw_sides(tree, "ordered_at")]
    widened = [t for t in others if LOW[:10] in t or HIGH[:10] in t]
    assert len(widened) == 2, sql
    assert not any(_ZONED_LITERAL.search(t) for t in widened), widened


@pytest.mark.parametrize("dialect", TIER1)
async def test_instant_pre_filter_literal_is_a_utc_instant(dialect: str) -> None:
    sql = await render(dialect, zoned_query(**_shapes("paid_at")["comparison"]))
    tree = parse(sql, dialect)
    others = [o.sql(dialect=dialect) for _s, o in raw_sides(tree, "paid_at")]
    widened = [t for t in others if LOW[:10] in t or HIGH[:10] in t]
    assert len(widened) == 2, sql
    assert all(_ZONED_LITERAL.search(t) for t in widened), widened


@pytest.mark.parametrize("dialect", TIER1)
async def test_no_pre_filter_without_a_zone(dialect: str) -> None:
    sql = await render(dialect, zoned_query(zone=None, **_shapes("ordered_at")["comparison"]))
    assert LOW[:10] not in sql, sql
    assert HIGH[:10] not in sql, sql


@pytest.mark.parametrize("dialect", ["sqlite", "duckdb", "postgres", "bigquery"])
async def test_not_on_frame_bounds(dialect: str) -> None:
    sql = await render(dialect, zoned_query(
        time_dimensions=[{"field": "orders.ordered_at", "granularity": "month"}],
        metrics=[m("sum(orders.amount, window='7d')", "w")], filter=_jan("ordered_at"),
    ))
    assert LOW[:10] not in sql, sql
    assert HIGH[:10] not in sql, sql


@pytest.mark.parametrize("backend", BACKENDS)
async def test_not_on_frame_bounds_executes(backend: str) -> None:
    async with tz_engine(backend) as engine:
        resp = await engine.execute(q(
            time_zone=NY, time_dimensions=[{"field": "orders.ordered_at", "granularity": "month"}],
            metrics=[m("sum(orders.amount, window='7d')", "w")], filter=_jan("ordered_at"),
        ))
    assert resp.sql is not None
    assert LOW[:10] not in resp.sql, resp.sql
    assert [str(r["orders.ordered_at"])[:7] for r in resp.data] == ["2025-01"], resp.data
