"""SQLite casts to timestamp and time types (spec: queries/casts)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import QueryTypeError
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2215_fixtures import BACKENDS, by_key, dev2215_engine, ids, q


@pytest.fixture
async def sqlite_engine():
    async with dev2215_engine("sqlite") as eng:
        yield eng


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with dev2215_engine(request.param) as eng:
        yield eng


def stamp(value: Any) -> Any:
    return None if value is None else str(value).replace("T", " ")


async def dimension(engine: SlayerQueryEngine, expr: str) -> dict[int, Any]:
    resp = await engine.execute(q(dimensions=["id", {"expression": expr, "name": "v"}]))
    return {int(k): stamp(v) for k, v in by_key(resp, "id").items()}


@pytest.mark.parametrize("expr", [
    "CAST(order_date AS TIMESTAMP)", "order_date::TIMESTAMP", "CAST(order_date AS DATETIME)",
    "CAST(order_date AS TIMESTAMP_NTZ)", "CAST(order_date AS TIMESTAMP WITHOUT TIME ZONE)",
])
async def test_timestamp_cast_of_a_date_column(sqlite_engine: SlayerQueryEngine, expr: str) -> None:
    assert (await dimension(sqlite_engine, expr))[3] == "2024-12-30 00:00:00"


@pytest.mark.parametrize("expr", [
    "CAST('2024-01-15 10:30:00' AS TIMESTAMP_NTZ)", "'2024-01-15 10:30:00'::DATETIME",
    "TRY_CAST('2024-01-15 10:30:00' AS TIMESTAMP)", "CAST('2024-01-15 10:30:00' AS TIMESTAMP)",
])
async def test_timestamp_spellings_agree(sqlite_engine: SlayerQueryEngine, expr: str) -> None:
    assert (await dimension(sqlite_engine, expr))[1] == "2024-01-15 10:30:00"


async def test_non_temporal_input_is_null(sqlite_engine: SlayerQueryEngine) -> None:
    got = await dimension(sqlite_engine, "CAST(note AS TIMESTAMP)")
    assert got[1] is None
    assert got[5] is None


async def test_timestamp_cast_in_a_filter(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(q(dimensions=["id"], filters=["created_at <= CAST('2024-04-10' AS TIMESTAMP)"]))
    assert ids(resp) == {1}


@pytest.mark.parametrize(("expr", "type_name"), [
    ("CAST(created_at AS TIME)", "time"), ("created_at::TIME", "time"), ("TRY_CAST(created_at AS TIME)", "time"),
    ("CAST(created_at AS TIMESTAMP(3))", "timestamp"), ("TRY_CAST(created_at AS TIMESTAMP(3))", "timestamp"),
    ("created_at::TIMESTAMP(3)", "timestamp"), ("created_at::TIMESTAMP WITH TIME ZONE", "timestamp"),
    ("CAST(created_at AS TIMESTAMP WITH TIME ZONE)", "timestamp"),
    ("TRY_CAST(created_at AS TIMESTAMP WITH TIME ZONE)", "timestamp"),
])
async def test_inexpressible_types_are_refused(sqlite_engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch,
                                               expr: str, type_name: str) -> None:
    calls: list[str] = []
    orig = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await orig(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    with pytest.raises(QueryTypeError) as info:
        await dimension(sqlite_engine, expr)
    msg = str(info.value).lower()
    assert type_name in msg, msg
    assert "sqlite" in msg, msg
    assert [s for s in calls if "slayer_rk_" not in s] == []
