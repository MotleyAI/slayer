"""An engine built without a clock pins "now" to ``SLAYER_NOW``, read once at construction."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from slayer.core.errors import SlayerError
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._slayer_now_fixtures import LAST_3_MONTHS, PIN, PINNED_WINDOW, ids, ids_in, now_engine
from tests._time_points_fixtures import month_key, monthly

D = datetime
EXPLICIT_NOW = D(2026, 9, 29, 12)
EXPLICIT_WINDOW = ids_in(D(2026, 6, 1), D(2026, 9, 1))
COUNT = [{"formula": "count(*)", "name": "n"}]


class _FalseyClock:
    """A clock callable that is falsey in a boolean context."""

    def __bool__(self) -> bool:
        return False

    def __call__(self) -> datetime:
        return EXPLICIT_NOW


async def test_pinned_datetime_drives_relative_tokens(monkeypatch) -> None:
    monkeypatch.setenv("SLAYER_NOW", PIN)
    async with now_engine() as engine:
        got = await ids(engine, LAST_3_MONTHS)
    assert got == PINNED_WINDOW
    assert {2, 4} <= got
    assert not {1, 5} & got


async def test_date_only_pins_midnight(monkeypatch) -> None:
    monkeypatch.setenv("SLAYER_NOW", "2025-07-15")
    async with now_engine() as engine:
        today = await ids(engine, "ts = 'today'")
        last_6_hours = await ids(engine, "ts = 'last 6 hours'")
    assert today == ids_in(D(2025, 7, 15), D(2025, 7, 16)) == {9, 10}
    assert last_6_hours == ids_in(D(2025, 7, 14, 18), D(2025, 7, 15)) == {7, 8}


@pytest.mark.parametrize("value", [None, "", "   "], ids=["unset", "empty", "whitespace"])
async def test_unset_or_blank_keeps_host_clock(monkeypatch, value) -> None:
    if value is None:
        monkeypatch.delenv("SLAYER_NOW", raising=False)
    else:
        monkeypatch.setenv("SLAYER_NOW", value)
    before = date.today()
    async with now_engine() as engine:
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "ev", "measures": COUNT, "filters": ["ts = 'today'"],
        }), dry_run=True)
    assert resp.sql is not None
    assert any(
        d.isoformat() in resp.sql and (d + timedelta(days=1)).isoformat() in resp.sql
        for d in {before, date.today()}
    ), resp.sql


@pytest.mark.parametrize("value", [PIN, "yesterday-ish", "2025-07-15T12:00:00Z"], ids=["pin", "unparseable", "aware"])
async def test_explicit_clock_wins(monkeypatch, value) -> None:
    monkeypatch.setenv("SLAYER_NOW", value)
    async with now_engine(clock=lambda: EXPLICIT_NOW) as engine:
        assert await ids(engine, LAST_3_MONTHS) == EXPLICIT_WINDOW == {14}


async def test_falsey_explicit_clock_wins(monkeypatch) -> None:
    monkeypatch.setenv("SLAYER_NOW", PIN)
    async with now_engine(clock=_FalseyClock()) as engine:
        assert await ids(engine, LAST_3_MONTHS) == EXPLICIT_WINDOW


@pytest.mark.parametrize("value", ["2025-07-15T12:00:00Z", "2025-07-15T12:00:00+02:00"], ids=["Z", "offset"])
def test_timezone_aware_value_rejected_at_build(monkeypatch, tmp_path, value) -> None:
    monkeypatch.setenv("SLAYER_NOW", value)
    with pytest.raises(SlayerError) as exc:
        SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path)))
    msg = str(exc.value)
    assert "SLAYER_NOW" in msg
    assert value in msg
    assert "naive" in msg.lower()


def test_unparseable_value_rejected_at_build(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("SLAYER_NOW", "yesterday-ish")
    with pytest.raises(SlayerError) as exc:
        SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path)))
    assert "SLAYER_NOW" in str(exc.value)
    assert "yesterday-ish" in str(exc.value)


async def test_variable_is_read_when_the_engine_is_built(monkeypatch) -> None:
    monkeypatch.setenv("SLAYER_NOW", PIN)
    async with now_engine() as first:
        monkeypatch.setenv("SLAYER_NOW", "2026-01-10T09:00:00")
        assert await ids(first, LAST_3_MONTHS) == PINNED_WINDOW
        second = SlayerQueryEngine(storage=first.storage)
        try:
            assert await ids(second, LAST_3_MONTHS) == ids_in(D(2025, 10, 1), D(2026, 1, 1)) == {12, 13}
        finally:
            second.close()


async def test_whole_periods_default_upper_bound_uses_pin(monkeypatch) -> None:
    monkeypatch.setenv("SLAYER_NOW", "2025-07-15T12:30:00")
    since_may = ["ts >= '2025-05-01'"]
    async with now_engine() as engine:
        pinned = await monthly(engine, measures=COUNT, filters=since_may, whole_periods_only=True)
        explicit_engine = SlayerQueryEngine(storage=engine.storage, clock=lambda: D(2025, 7, 15, 12, 30))
        try:
            explicit = await monthly(explicit_engine, measures=COUNT, filters=since_may, whole_periods_only=True)
        finally:
            explicit_engine.close()
    assert pinned == explicit
    assert set(pinned) == {"2025-05", "2025-06"}


def _spine_months(resp) -> set[str]:
    return {month_key(r["time_spine.timestamp"]) for r in resp.data}


async def test_time_spine_default_upper_bound_uses_pin(monkeypatch) -> None:
    monkeypatch.setenv("SLAYER_NOW", PIN)
    query = SlayerQuery.model_validate({
        "measures": [{"formula": "count(ev.id)", "name": "n"}],
        "time_dimensions": [{"dimension": "time_spine.timestamp", "granularity": "month", "date_range": ["2025-05-01", None]}],
    })
    async with now_engine() as engine:
        pinned = await engine.execute(query)
        explicit_engine = SlayerQueryEngine(storage=engine.storage, clock=lambda: D(2025, 7, 15, 12))
        try:
            explicit = await explicit_engine.execute(query)
        finally:
            explicit_engine.close()
    assert _spine_months(pinned) == _spine_months(explicit) == {"2025-05", "2025-06", "2025-07"}
