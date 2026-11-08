"""Queries that read the clock (now() / current_date()) bypass the result cache; other date queries still cache."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.cache import CacheConfig, QueryCache
from slayer.sql.client import SlayerSQLClient
from tests._dev1737_fixtures import exec_engine, scenario_models, scenario_tables

CLOCK_FILTER = "created_at >= date_add(current_date(), -30, 'day')"


class _Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture
async def engine():
    tables = scenario_tables(today=datetime.now(timezone.utc).date())
    async with exec_engine("sqlite", tables=tables, models=scenario_models()) as eng:
        yield eng


@pytest.fixture
def data_calls(monkeypatch) -> list[str]:
    calls: list[str] = []
    orig = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await orig(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    return calls


def _q(filters: list[str], **kw) -> SlayerQuery:
    return SlayerQuery.model_validate({
        "source_model": "orders", "measures": [{"formula": "count(*)", "name": "n"}], "filters": filters, **kw,
    })


def _data(calls: list[str]) -> list[str]:
    return [s for s in calls if "slayer_rk_" not in s]


class TestClockBypass:
    @pytest.mark.parametrize("query", [
        _q([CLOCK_FILTER]),
        _q(["created_at <= now()"]),
        _q([], dimensions=[{"expression": "date_diff('day', created_at, now())", "name": "age"}]),
        SlayerQuery.model_validate({
            "source_model": "orders", "measures": [{"formula": "max(date_diff('day', created_at, current_date()))", "name": "m"}],
        }),
    ])
    async def test_root_clock_query_never_cached(self, engine, data_calls, query) -> None:
        await engine.execute(query, cache=True)
        await engine.execute(query, cache=True)
        assert len(_data(data_calls)) == 2
        assert engine.cache_size == 0

    async def test_clock_inside_a_stage_never_cached(self, engine, data_calls) -> None:
        inner = SlayerQuery.model_validate({
            "name": "recent_orders", "source_model": "orders", "dimensions": ["status"],
            "measures": [{"formula": "count(*)", "name": "n"}], "filters": ["created_at <= now()"],
        })
        outer = SlayerQuery.model_validate({"source_model": "recent_orders", "measures": ["n:sum"]})
        await engine.execute([inner, outer], cache=True)
        await engine.execute([inner, outer], cache=True)
        assert len(_data(data_calls)) == 2
        assert engine.cache_size == 0

    async def test_plain_date_query_still_cached(self, engine, data_calls) -> None:
        query = _q(["date_diff('day', created_at, shipped_at) > 3"])
        first = await engine.execute(query, cache=True)
        second = await engine.execute(query, cache=True)
        assert len(_data(data_calls)) == 1
        assert engine.cache_size == 1
        assert first.data == second.data

    async def test_refresh_does_not_repopulate_a_clock_query(self, engine) -> None:
        clock = _Clock()
        engine._cache = QueryCache(config=CacheConfig(ttl_seconds=100), clock=clock)
        plain = {"source_model": "orders", "dimensions": ["status"], "measures": [{"formula": "count(*)", "name": "n"}]}
        await engine.create_model_from_query(query=plain, name="obs")
        await engine.execute("obs", cache=True)
        assert engine.cache_size == 1

        await engine.save_model(SlayerModel(name="obs", source_queries=[SlayerQuery.model_validate({
            **plain, "filters": ["created_at <= now()"],
        })]))
        clock.t += 200
        await engine.refresh()
        assert engine.cache_size == 0
        await engine.execute("obs", cache=True)
        assert engine.cache_size == 0
