"""Relative tokens resolve against one clock reading per execution; a new day yields new SQL."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.sql.client import SlayerSQLClient

from tests._dev1737_fixtures import seed_backend
from tests._engine_helpers import seeded_exec_engine
from tests._time_points_fixtures import CUSTOMERS, EV, NOW, PinnedClock, all_models, ids_in, tp_engine

D = datetime


def _recent_model() -> SlayerModel:
    """A query-backed model whose own filter uses a relative token."""
    return SlayerModel(
        name="recent", data_source="test",
        source_queries=[SlayerQuery.model_validate({
            "source_model": "ev", "dimensions": ["id"],
            "time_dimensions": [{"dimension": "ts", "granularity": "day"}],
            "measures": [{"formula": "sum(amount)", "name": "rev"}],
            "filters": ["ts >= 'last 3 months'"],
        })],
    )


def _stages() -> list[SlayerQuery]:
    inner = SlayerQuery.model_validate({
        "name": "inner", "source_model": "recent",
        "time_dimensions": [{"dimension": "ts", "granularity": "month", "date_range": [None, "last month"]}],
        "measures": [{"formula": "sum(rev)", "name": "r"}],
        "filters": ["ts < 'this month'"],
    })
    outer = SlayerQuery.model_validate({
        "source_model": "inner", "measures": [{"formula": "sum(r)", "name": "total"}],
        "filters": ["ts >= 'last 3 months'"],
    })
    return [inner, outer]


@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
async def test_clock_read_once_per_execution(backend) -> None:
    clock = PinnedClock(NOW, step=timedelta(days=40))
    async with tp_engine(backend, clock=clock) as engine:
        await engine.save_model(_recent_model())
        clock.calls = 0
        resp = await engine.execute(_stages())
    assert clock.calls == 1
    # Every stage and the spliced model resolve against 2026-09-29: [2026-06-01, 2026-09-01).
    assert resp.data[0]["inner.total"] == pytest.approx(float(sum(ids_in(D(2026, 6, 1), D(2026, 9, 1)))))


async def test_dry_run_reads_the_clock_once() -> None:
    clock = PinnedClock(NOW, step=timedelta(days=40))
    async with tp_engine("sqlite", clock=clock) as engine:
        await engine.save_model(_recent_model())
        clock.calls = 0
        await engine.execute(_stages(), dry_run=True)
    assert clock.calls == 1


async def test_new_day_yields_new_sql() -> None:
    clock = PinnedClock(NOW)
    query = SlayerQuery.model_validate({
        "source_model": "ev", "measures": [{"formula": "count(*)", "name": "n"}],
        "filters": ["ts >= 'last 7 days'"],
    })
    async with tp_engine("sqlite", clock=clock) as engine:
        first = (await engine.execute(query, dry_run=True)).sql
        clock.now = NOW + timedelta(days=1)
        second = (await engine.execute(query, dry_run=True)).sql
    assert first and second and first != second
    assert "2026-09-22" in first and "2026-09-23" in second


async def test_cache_never_serves_another_days_result() -> None:
    clock = PinnedClock(NOW)
    query = SlayerQuery.model_validate({"source_model": "ev", "dimensions": ["id"], "filters": ["ts = 'today'"]})
    async with tp_engine("sqlite", clock=clock) as engine:
        first = await engine.execute(query, cache=True)
        clock.now = D(2026, 9, 28, 12)
        second = await engine.execute(query, cache=True)
    assert {r["ev.id"] for r in first.data} == ids_in(D(2026, 9, 29), D(2026, 9, 30))
    assert {r["ev.id"] for r in second.data} == ids_in(D(2026, 9, 28), D(2026, 9, 29))


async def test_same_day_relative_query_is_served_from_cache(monkeypatch) -> None:
    calls: list[str] = []
    orig = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await orig(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    clock = PinnedClock(NOW)
    query = SlayerQuery.model_validate({
        "source_model": "ev", "measures": [{"formula": "count(*)", "name": "n"}], "filters": ["ts >= 'last 7 days'"],
    })
    async with tp_engine("sqlite", clock=clock) as engine:
        first = await engine.execute(query, cache=True)
        clock.now = NOW + timedelta(hours=3)
        second = await engine.execute(query, cache=True)
        assert engine.cache_size == 1
    assert len([s for s in calls if "slayer_rk_" not in s]) == 1
    assert first.data == second.data


async def test_default_clock_is_host_local_now() -> None:
    before = date.today()
    async with seeded_exec_engine(
        dialect="sqlite", seed=lambda p: seed_backend("sqlite", p, [EV, CUSTOMERS]), models=all_models(),
    ) as (engine, _db):
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "ev", "measures": [{"formula": "count(*)", "name": "n"}],
            "filters": ["ts = 'today'"],
        }), dry_run=True)
    assert resp.sql is not None
    assert any(
        d.isoformat() in resp.sql and (d + timedelta(days=1)).isoformat() in resp.sql
        for d in {before, date.today()}
    ), resp.sql
