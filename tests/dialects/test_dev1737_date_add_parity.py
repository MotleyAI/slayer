"""One date-arithmetic primitive per dialect: time_shift, Sunday-week truncation and window frames all use build_date_add."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, SlayerModel
from slayer.sql.dialects import SqlDialect, get_dialect
from tests._engine_helpers import _engine_generate
from tests._golden_harness import bind_golden_tests, record_raise

GOLDEN_PATH = Path(__file__).parent.parent / "golden" / "dev1737_date_add_parity.json"
TIER1 = ["sqlite", "postgres", "duckdb", "mysql", "clickhouse", "tsql", "snowflake", "bigquery"]
TG = TimeGranularity
SHIFT_GRANULARITIES = [TG.HOUR, TG.DAY, TG.WEEK, TG.WEEK_SUNDAY, TG.MONTH, TG.QUARTER, TG.YEAR]


def _ev_model() -> SlayerModel:
    return SlayerModel(
        name="ev", sql_table="ev", data_source="test", default_time_dimension="ts",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="ts", type=DataType.TIMESTAMP),
            Column(name="v", type=DataType.DOUBLE),
        ],
    )


def _query(granularity: str, *measures) -> dict:
    return {
        "source_model": "ev",
        "time_dimensions": [{"dimension": "ts", "granularity": granularity}],
        "measures": list(measures),
    }


def _shift(granularity: TimeGranularity, offset: int, *, bucket: TimeGranularity | None = None) -> dict:
    return _query((bucket or granularity).value, "v:sum", {
        "formula": f"time_shift(v:sum, {offset}, '{granularity.value}')", "name": "x"})


def _window(duration: str, bucket: str) -> dict:
    return _query(bucket, {"formula": f"v:sum(window='{duration}')", "name": "x"})


def _cases() -> dict:
    cases = {
        f"shift|{g.value}|{o}": _shift(g, o) for g in SHIFT_GRANULARITIES for o in (-1, 1)
    }
    cases.update({
        "shift|day_on_month|-1": _shift(TG.DAY, -1, bucket=TG.MONTH),
        "trunc|week_sunday": _query("week_sunday", "v:sum"),
        "window|1m|day": _window("1m", "day"),
        "window|90d|month": _window("90d", "month"),
        "window|2w|week": _window("2w", "week"),
        "window|1y2m3w5d6h7min8s|day": _window("1y2m3w5d6h7min8s", "day"),
    })
    return cases


async def _generate_one(query: dict, dialect: str):
    try:
        return await _engine_generate(query=[query], model=_ev_model(), dialect=dialect)
    except Exception as exc:  # noqa: BLE001 — the raise itself is the contract
        return record_raise(exc)


ALLOWED_DELTAS: dict[str, str] = {}  # PENDING re-bless list; empty in a committed state.

bind_golden_tests(
    namespace=globals(),
    golden_path=GOLDEN_PATH,
    cases=_cases,
    dialects=TIER1,
    allowed=ALLOWED_DELTAS,
    generate_one=_generate_one,
)


def test_every_case_generates_sql(baseline) -> None:
    raised = {k: v for k, v in baseline.items() if isinstance(v, dict)}
    assert not raised, raised


@pytest.mark.parametrize("hook", ["build_time_offset_expr", "duration_interval_exprs"])
def test_parallel_date_arithmetic_hooks_are_gone(hook: str) -> None:
    assert not hasattr(SqlDialect, hook)
    for name in TIER1:
        assert not hasattr(get_dialect(name), hook), name


# ---------------------------------------------------------------------------
# Spy: every calendar offset goes through build_date_add.
# ---------------------------------------------------------------------------

_MONTHS = {TG.MONTH: 1, TG.QUARTER: 3, TG.YEAR: 12}
_DAYS = {TG.DAY: 1, TG.WEEK: 7, TG.WEEK_SUNDAY: 7}
_SECONDS = {TG.SECOND: 1, TG.MINUTE: 60, TG.HOUR: 3600}


def _span(unit: TimeGranularity, count: int) -> tuple[str, int]:
    for kind, table in (("months", _MONTHS), ("days", _DAYS), ("seconds", _SECONDS)):
        if unit in table:
            return kind, count * table[unit]
    raise AssertionError(unit)


def _count(node: exp.Expression) -> int:
    return int(node.sql().strip("()"))


@pytest.fixture
def spy(monkeypatch):
    calls: list[tuple[str, int]] = []

    def install(dialect: str) -> list[tuple[str, int]]:
        cls = type(get_dialect(dialect))
        orig = cls.build_date_add
        sig = inspect.signature(orig)

        def recording(self, *args, **kwargs):
            bound = sig.bind(self, *args, **kwargs)
            calls.append(_span(TimeGranularity(bound.arguments["unit"]), _count(bound.arguments["count"])))
            return orig(self, *args, **kwargs)

        monkeypatch.setattr(cls, "build_date_add", recording)
        return calls

    return install


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("granularity", SHIFT_GRANULARITIES)
async def test_time_shift_uses_date_add(spy, dialect: str, granularity: TimeGranularity) -> None:
    calls = spy(dialect)
    await _engine_generate(query=[_shift(granularity, -1)], model=_ev_model(), dialect=dialect)
    kind, amount = _span(granularity, 1)
    assert (kind, amount) in calls or (kind, -amount) in calls, calls


@pytest.mark.parametrize("dialect", [d for d in TIER1 if d != "bigquery"])
async def test_sunday_week_truncation_uses_date_add(spy, dialect: str) -> None:
    calls = spy(dialect)
    await _engine_generate(query=[_query("week_sunday", "v:sum")], model=_ev_model(), dialect=dialect)
    assert ("days", 1) in calls and ("days", -1) in calls, calls


@pytest.mark.parametrize("dialect", TIER1)
async def test_window_frame_parts_apply_in_written_order(spy, dialect: str) -> None:
    calls = spy(dialect)
    await _engine_generate(query=[_window("1y2m3w5d6h7min8s", "day")], model=_ev_model(), dialect=dialect)
    assert ("days", 1) in calls, calls
    expected = [("months", -12), ("months", -2), ("days", -21), ("days", -5),
                ("seconds", -21600), ("seconds", -420), ("seconds", -8)]
    it = iter(calls)
    assert all(step in it for step in expected), calls
