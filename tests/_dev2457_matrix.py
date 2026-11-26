"""The window case table every Tier-1 database runs (spec: queries/window-functions › Every database runs the
required windows or refuses with a typed error).

Expected values are hand-computed from the rows of ``tests/_dev2457_fixtures.py``.
"""

from __future__ import annotations

import math
import re
from typing import Any, Literal, Optional

import pytest
import sqlglot
from pydantic import BaseModel, ConfigDict
from sqlglot import exp

from slayer.core.errors import IllegalWindowInFilterError, QueryTypeError, UnevaluableEntityError
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2101_fixtures import emit
from tests._dev2457_fixtures import AMOUNT_OF_ID, by, raw, run, window_datasets

ORDER_IDS = tuple(range(1, 8))
#: Population standard deviation of a three-row region (amounts 10 apart).
_SD_POP = math.sqrt(200 / 3)


class WindowCase(BaseModel):
    """One raw-row window: the value per row id of ``population`` (``None`` is NULL)."""

    model_config = ConfigDict(frozen=True)
    case_id: str
    population: str
    expression: str
    expected: dict[int, Optional[float]]
    kind: Literal["int", "float"] = "int"

    def body(self) -> dict[str, Any]:
        return raw(self.population, self.expression)

    def query(self) -> SlayerQuery:
        return SlayerQuery.model_validate(self.body())


def _orders(case_id: str, expression: str, values: list[Any], *, kind: Literal["int", "float"] = "int") -> WindowCase:
    return WindowCase(case_id=case_id, population="orders", expression=expression,
                      expected=dict(zip(ORDER_IDS, values, strict=True)), kind=kind)


def _rows(case_id: str, population: str, expression: str, values: list[Any],
          *, kind: Literal["int", "float"] = "int") -> WindowCase:
    return WindowCase(case_id=case_id, population=population, expression=expression,
                      expected=dict(zip(range(1, len(values) + 1), values, strict=True)), kind=kind)


def _per_region(eu_us: Any, asia: Any) -> list[Any]:
    """Orders 1–6 are EU and US (three rows each, same spread); order 7 is the one AS row."""
    return [eu_us] * 6 + [asia]


def _ranking_cases() -> list[WindowCase]:
    return [
        _orders("rank|row_number", "ROW_NUMBER() OVER (ORDER BY orders.amount)", [1, 2, 3, 4, 5, 6, 7]),
        _orders("rank|rank", "RANK() OVER (ORDER BY orders.ordered_at)", [1, 4, 6, 1, 4, 6, 1]),
        _orders("rank|dense_rank", "DENSE_RANK() OVER (ORDER BY orders.ordered_at)", [1, 2, 3, 1, 2, 3, 1]),
        _orders("rank|ntile", "NTILE(2) OVER (ORDER BY orders.amount)", [1, 1, 1, 1, 2, 2, 2]),
        _orders("rank|percent_rank", "PERCENT_RANK() OVER (ORDER BY orders.amount)",
                [(i - 1) / 6 for i in ORDER_IDS], kind="float"),
        _orders("rank|cume_dist", "CUME_DIST() OVER (ORDER BY orders.amount)",
                [i / 7 for i in ORDER_IDS], kind="float"),
        _orders("rank|frame-changes-nothing",
                "RANK() OVER (ORDER BY orders.amount ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)", [1, 2, 3, 4, 5, 6, 7]),
    ]


def _offset_cases() -> list[WindowCase]:
    amounts = [AMOUNT_OF_ID[i] for i in ORDER_IDS]
    return [
        _orders("offset|lag-default", "LAG(orders.amount, 1, 0) OVER (ORDER BY orders.amount)",
                [0, 10, 20, 30, 40, 50, 60]),
        _orders("offset|lead-default", "LEAD(orders.amount, 1, 0) OVER (ORDER BY orders.amount)",
                [20, 30, 40, 50, 60, 70, 0]),
        _orders("offset|lag", "LAG(orders.amount) OVER (ORDER BY orders.amount)", [None, 10, 20, 30, 40, 50, 60]),
        _orders("offset|first_value", "FIRST_VALUE(orders.amount) OVER (ORDER BY orders.amount)", [10] * 7),
        _orders("offset|last_value", "LAST_VALUE(orders.amount) OVER (ORDER BY orders.amount)", amounts),
        _orders("offset|nth_value", "NTH_VALUE(orders.amount, 2) OVER (ORDER BY orders.amount)", [None] + [20] * 6),
    ]


def _aggregation_cases() -> list[WindowCase]:
    by_region = "OVER (PARTITION BY orders.region)"
    return [
        _orders("agg|sum", f"SUM(orders.amount) {by_region}", [60, 60, 60, 150, 150, 150, 70]),
        _orders("agg|count-star", f"COUNT(*) {by_region}", _per_region(3, 1)),
        _orders("agg|share-of-total", "orders.amount / SUM(orders.amount) OVER () * 100",
                [AMOUNT_OF_ID[i] / 2.8 for i in ORDER_IDS], kind="float"),
        _orders("agg|avg-rows-frame", "AVG(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)",
                [10, 15, 20, 25, 30, 35, 40], kind="float"),
        _orders("agg|sum-filter", f"SUM(orders.amount) FILTER (WHERE orders.amount > 10) {by_region}",
                [50, 50, 50, 150, 150, 150, 70]),
        _orders("agg|count", f"COUNT(orders.amount) {by_region}", _per_region(3, 1)),
        _orders("agg|avg", f"AVG(orders.amount) {by_region}", [20, 20, 20, 50, 50, 50, 70], kind="float"),
        _orders("agg|min", f"MIN(orders.amount) {by_region}", [10, 10, 10, 40, 40, 40, 70]),
        _orders("agg|max", f"MAX(orders.amount) {by_region}", [30, 30, 30, 60, 60, 60, 70]),
        _orders("agg|stddev", f"STDDEV(orders.amount) {by_region}", _per_region(10, None), kind="float"),
        _orders("agg|stddev_pop", f"STDDEV_POP(orders.amount) {by_region}", _per_region(_SD_POP, 0), kind="float"),
        _orders("agg|stddev_samp", f"STDDEV_SAMP(orders.amount) {by_region}", _per_region(10, None), kind="float"),
        _orders("agg|variance", f"VARIANCE(orders.amount) {by_region}", _per_region(100, None), kind="float"),
        _orders("agg|var_pop", f"VAR_POP(orders.amount) {by_region}", _per_region(200 / 3, 0), kind="float"),
        _orders("agg|var_samp", f"VAR_SAMP(orders.amount) {by_region}", _per_region(100, None), kind="float"),
    ]


def _frame_cases() -> list[WindowCase]:
    return [
        _orders("frame|rows-running",
                "SUM(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                [10, 30, 60, 100, 150, 210, 280]),
        _orders("frame|rows-centred",
                "SUM(orders.amount) OVER (ORDER BY orders.id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
                [30, 60, 90, 120, 150, 180, 130]),
        _orders("frame|range-running",
                "SUM(orders.amount) OVER (ORDER BY orders.ordered_at RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
                [120, 190, 280, 120, 190, 280, 120]),
    ]


def _null_placement_cases() -> list[WindowCase]:
    return [
        _rows("nulls|default-asc", "scores", "ROW_NUMBER() OVER (ORDER BY scores.score)", [2, 3, 1]),
        _rows("nulls|default-desc", "scores", "ROW_NUMBER() OVER (ORDER BY scores.score DESC)", [2, 1, 3]),
        _rows("nulls|stated-first", "scores", "ROW_NUMBER() OVER (ORDER BY scores.score NULLS FIRST)", [3, 1, 2]),
        _rows("nulls|stated-desc-last", "scores", "ROW_NUMBER() OVER (ORDER BY scores.score DESC NULLS LAST)", [1, 3, 2]),
    ]


def _peer_cases() -> list[WindowCase]:
    return [
        _rows("peers|rank", "ties", "RANK() OVER (ORDER BY ties.a, ties.b DESC)", [3, 3, 1, 2]),
        _rows("peers|dense_rank", "ties", "DENSE_RANK() OVER (ORDER BY ties.a, ties.b DESC)", [3, 3, 1, 2]),
        _rows("peers|cume_dist", "ties", "CUME_DIST() OVER (ORDER BY ties.a)", [1, 1, 0.5, 0.5], kind="float"),
        _rows("peers|count-range", "ties",
              "COUNT(*) OVER (ORDER BY ties.a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", [4, 4, 2, 2]),
        _rows("peers|cume_dist-two-keys", "ties", "CUME_DIST() OVER (ORDER BY ties.a, ties.b DESC)",
              [1, 1, 0.25, 0.5], kind="float"),
        _rows("peers|count-range-two-keys", "ties",
              "COUNT(*) OVER (ORDER BY ties.a, ties.b DESC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
              [4, 4, 1, 2]),
    ]


def window_cases() -> list[WindowCase]:
    return [*_ranking_cases(), *_offset_cases(), *_aggregation_cases(), *_frame_cases(),
            *_null_placement_cases(), *_peer_cases()]


CASES = window_cases()
CASE_IDS = [c.case_id for c in CASES]

MEDIAN_OVER = "MEDIAN(orders.amount) OVER ()"
COUNT_DISTINCT_OVER = "COUNT(DISTINCT orders.region) OVER ()"
PERCENTILE_CONT_OVER = "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY orders.amount) OVER ()"
#: (window, pattern naming its function) for the holistic aggregates a database may not window.
HOLISTIC = ((MEDIAN_OVER, "(?i)median"), (COUNT_DISTINCT_OVER, "(?i)count.*distinct"),
            (PERCENTILE_CONT_OVER, "(?i)percentile_cont"))
HOLISTIC_IDS = ["median", "count-distinct", "percentile_cont"]
#: The holistic windows DuckDB evaluates; the median of amounts 10..70 is 40.
DUCKDB_HOLISTIC = (MEDIAN_OVER, PERCENTILE_CONT_OVER)


def assert_case(data: list[dict[str, Any]], case: WindowCase) -> None:
    got = {int(row["id"]): row["v0"] for row in data}
    assert len(got) == len(data), f"{case.case_id}: duplicate ids in {data}"
    assert set(got) == set(case.expected), f"{case.case_id}: row ids {sorted(got)}"
    for rid, want in case.expected.items():
        have = got[rid]
        where = f"{case.case_id} id={rid}"
        if want is None:
            assert have is None, f"{where}: expected NULL, got {have!r}"
            continue
        assert have is not None, f"{where}: expected {want!r}, got NULL"
        assert not isinstance(have, (str, bool)), f"{where}: expected a number, got {have!r}"
        if case.kind == "int":
            assert float(have) == want, f"{where}: expected {want!r}, got {have!r}"
        else:
            assert math.isclose(float(have), want, rel_tol=1e-9, abs_tol=1e-9), f"{where}: expected {want!r}, got {have!r}"


async def check_case(engine: SlayerQueryEngine, case: WindowCase) -> None:
    resp = await run(engine, case.body())
    try:
        assert_case(resp.data, case)
    except AssertionError as exc:
        raise AssertionError(f"{exc}\nSQL:\n{resp.sql}") from None


async def emit_window(case: WindowCase, *, dialect: str) -> str:
    """Dry-run SQL of ``case`` on ``dialect`` (asserts it parses there)."""
    return await emit(case.query(), dialect=dialect, datasets=window_datasets())


def holds_window(sql: str, *, dialect: str) -> bool:
    return any(tree is not None and tree.find(exp.Window) is not None for tree in sqlglot.parse(sql, dialect=dialect))


async def window_values(engine: SlayerQueryEngine, population: str, expression: str) -> dict[int, Any]:
    return {int(k): v for k, v in by(await run(engine, raw(population, expression))).items()}


async def assert_typed_refusal(engine: SlayerQueryEngine, population: str, expression: str, *names: str) -> None:
    """``expression`` is refused by the type checker, the message matching each of ``names``."""
    with pytest.raises(QueryTypeError) as info:
        await run(engine, raw(population, expression))
    assert not isinstance(info.value, (IllegalWindowInFilterError, UnevaluableEntityError)), repr(info.value)
    message = str(info.value)
    for name in names:
        assert re.search(name, message), f"{name!r} not in: {message}"
