"""Casts to and from a time of day (spec: queries/time-of-day › Casts, queries/casts)."""

from __future__ import annotations

from datetime import time

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import CastTypeError
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2120_fixtures import patch_profile
from tests._ossie_gate import TIER1
from tests._time_of_day_fixtures import BACKENDS, NY, d, emit, is_time, planned_type, q, shifts_engine, value_of

TEXT_SPELLINGS = ["CAST('10:30:00' AS TIME)", "'10:30:00'::TIME", "TRY_CAST('10:30:00' AS TIME)",
                  "CAST('10:30' AS TIME)", "CAST('10:30:' || '00' AS TIME)"]
#: (expression, the source type the error names; None for a literal, whose type name is not pinned).
REFUSED_SOURCES = [
    ("CAST(shifts.order_date AS TIME)", "Date"), ("shifts.order_date::TIME", "Date"),
    ("TRY_CAST(shifts.order_date AS TIME)", "Date"), ("CAST(DATE '2024-01-15' AS TIME)", "Date"),
    ("CAST(shifts.amount AS TIME)", "Float"), ("CAST(shifts.id AS TIME)", "Integer"), ("CAST(3600 AS TIME)", None),
    ("TRY_CAST(1.5 AS TIME)", None), ("CAST(shifts.is_paid AS TIME)", "Boolean"), ("CAST(TRUE AS TIME)", "Boolean"),
    ("CAST(['10:30:00'] AS TIME)", "Array"),
]
REFUSED_TARGETS = ["TIMESTAMP", "DATE", "INTEGER", "DOUBLE", "BOOLEAN", "DECIMAL(10,2)"]


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with shifts_engine(request.param) as eng:
        yield eng


@pytest.fixture
async def sqlite_engine():
    async with shifts_engine("sqlite") as eng:
        yield eng


@pytest.fixture
def data_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    real = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await real(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    return calls


def _data(calls: list[str]) -> list[str]:
    return [s for s in calls if "slayer_rk_" not in s]


class TestPlannedType:
    @pytest.mark.parametrize("text", [
        "CAST(shifts.label AS TIME)", "shifts.label::TIME", "TRY_CAST(shifts.label AS TIME)",
        "CAST(shifts.label AS TIME(3))", "CAST(shifts.label AS TIME WITHOUT TIME ZONE)",
        "CAST(shifts.created_at AS TIME)", "CAST(shifts.starts_at AS TIME)", "min(CAST(shifts.label AS TIME))",
    ])
    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb"])
    def test_a_time_cast_is_a_time_of_day(self, dialect: str, text: str) -> None:
        assert planned_type(text, dialect=dialect) is DataType.TIME

    @pytest.mark.parametrize("text", ["CAST(shifts.label AS TIME WITH TIME ZONE)", "CAST(shifts.label AS TIMETZ)",
                                      "CAST(shifts.label AS TIME(3) WITH TIME ZONE)"])
    def test_a_zoned_time_cast_stays_opaque(self, text: str) -> None:
        assert planned_type(text, dialect="postgres") is DataType.OPAQUE

    def test_a_time_cast_to_text_is_text(self) -> None:
        assert planned_type("CAST(shifts.starts_at AS VARCHAR)") is DataType.STRING


class TestToTime:
    @pytest.mark.parametrize("text", TEXT_SPELLINGS)
    async def test_text(self, engine: SlayerQueryEngine, text: str) -> None:
        got = await value_of(engine, text)
        assert is_time(got, time(10, 30)), got

    async def test_text_keeps_its_fraction(self, engine: SlayerQueryEngine) -> None:
        got = await value_of(engine, "CAST('10:30:00.123456' AS TIME)")
        assert is_time(got, time(10, 30, 0, 123456)), got

    @pytest.mark.parametrize("text", ["CAST(shifts.created_at AS TIME)", "shifts.created_at::TIME",
                                      "TRY_CAST(shifts.created_at AS TIME)",
                                      "CAST(TIMESTAMP '2024-01-15 10:30:45' AS TIME)"])
    async def test_timestamp(self, engine: SlayerQueryEngine, text: str) -> None:
        got = await value_of(engine, text)
        assert is_time(got, time(10, 30, 45)), got

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_zoned_timestamp_is_read_in_the_query_zone(self, backend: str) -> None:
        async with shifts_engine(backend) as eng:
            zoned = await value_of(eng, "CAST(shifts.seen_at AS TIME)", time_zone=NY)
            utc = await value_of(eng, "CAST(shifts.seen_at AS TIME)", time_zone="UTC")
        assert is_time(zoned, time(22, 30)), zoned
        assert is_time(utc, time(3, 30)), utc

    async def test_time_to_itself(self, engine: SlayerQueryEngine) -> None:
        got = await value_of(engine, "CAST(shifts.starts_at AS TIME)")
        assert is_time(got, time(9, 30)), got

    async def test_in_a_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["shifts.id"],
                                      filter=["CAST(shifts.created_at AS TIME) > TIME '09:00:00'"]))
        assert {int(r["shifts.id"]) for r in resp.data} == {1, 3}


class TestRefusedSources:
    @pytest.mark.parametrize(("text", "source"), REFUSED_SOURCES, ids=[t for t, _ in REFUSED_SOURCES])
    async def test_refused_before_the_database(self, engine: SlayerQueryEngine, data_calls: list[str], text: str,
                                               source: str | None) -> None:
        with pytest.raises(CastTypeError) as info:
            await value_of(engine, text)
        assert (source or "time").lower() in str(info.value).lower(), str(info.value)
        assert _data(data_calls) == []

    @pytest.mark.parametrize("dialect", TIER1)
    @pytest.mark.parametrize("text", ["CAST(shifts.order_date AS TIME)", "CAST(shifts.amount AS TIME)",
                                      "CAST(shifts.is_paid AS TIME)"])
    async def test_refused_on_every_database(self, monkeypatch: pytest.MonkeyPatch, dialect: str, text: str) -> None:
        patch_profile(monkeypatch, version=(99, 0, 0))
        query = q(dimensions=[d(text)])
        with pytest.raises(CastTypeError):
            await emit(query, dialect=dialect)


class TestInvalidText:
    @pytest.mark.parametrize("text", ["TRY_CAST('bogus' AS TIME)", "TRY_CAST('25:00:00' AS TIME)",
                                      "TRY_CAST('24:00:00' AS TIME)", "TRY_CAST('10:60:00' AS TIME)",
                                      "TRY_CAST(shifts.label AS TIME)", "TRY_CAST(CAST(NULL AS VARCHAR) AS TIME)"])
    async def test_try_cast_is_null(self, engine: SlayerQueryEngine, text: str) -> None:
        resp = await engine.execute(q(dimensions=[d(text)], filter=["shifts.id = 1"]))
        assert [r["v"] for r in resp.data] == [None], resp.sql

    @pytest.mark.parametrize("text", ["CAST('bogus' AS TIME)", "'25:00:00'::TIME", "CAST('24:00:00' AS TIME)",
                                      "CAST(shifts.label AS TIME)"])
    async def test_cast_on_sqlite_is_null(self, sqlite_engine: SlayerQueryEngine, text: str) -> None:
        assert await value_of(sqlite_engine, text) is None


class TestFromTime:
    @pytest.mark.parametrize(("text", "expected"), [
        ("CAST(TIME '10:30:00' AS VARCHAR)", "10:30:00"), ("CAST(TIME '10:30:00.5' AS VARCHAR)", "10:30:00.5"),
        ("TIME '10:30:00.120000'::TEXT", "10:30:00.12"), ("CAST(TIME '10:30' AS STRING)", "10:30:00"),
        ("CAST(shifts.starts_at AS VARCHAR)", "09:30:00"), ("TRY_CAST(shifts.starts_at AS TEXT)", "09:30:00"),
    ])
    async def test_to_text(self, engine: SlayerQueryEngine, text: str, expected: str) -> None:
        got = await value_of(engine, text)
        assert got == expected, got

    @pytest.mark.parametrize("backend", BACKENDS)
    async def test_stored_fraction_to_text(self, backend: str) -> None:
        async with shifts_engine(backend) as eng:
            assert await value_of(eng, "CAST(shifts.ends_at AS VARCHAR)", row=3) == "23:00:00.25"

    @pytest.mark.parametrize("target", REFUSED_TARGETS)
    @pytest.mark.parametrize("source", ["TIME '10:30:00'", "shifts.starts_at", "current_time()"])
    async def test_to_another_type_is_refused(self, engine: SlayerQueryEngine, data_calls: list[str], source: str,
                                              target: str) -> None:
        with pytest.raises(CastTypeError) as info:
            await value_of(engine, f"CAST({source} AS {target})")
        assert "time" in str(info.value).lower(), str(info.value)
        assert _data(data_calls) == []

    async def test_try_cast_to_another_type_is_refused(self, engine: SlayerQueryEngine) -> None:
        with pytest.raises(CastTypeError):
            await value_of(engine, "TRY_CAST(shifts.starts_at AS INTEGER)")


class TestPrecisionAndZone:
    async def test_postgres_keeps_the_precision(self) -> None:
        sql = await emit(q(dimensions=[d("CAST(shifts.label AS TIME(3))")]), dialect="postgres")
        assert "TIME(3)" in sql.upper().replace(" ", ""), sql

    @pytest.mark.parametrize("text", ["CAST(shifts.created_at AS TIME WITH TIME ZONE)",
                                      "TRY_CAST(shifts.created_at AS TIME WITH TIME ZONE)",
                                      "shifts.created_at::TIME WITH TIME ZONE"])
    async def test_sqlite_refuses_a_zoned_time(self, sqlite_engine: SlayerQueryEngine, data_calls: list[str],
                                               text: str) -> None:
        with pytest.raises(CastTypeError) as info:
            await value_of(sqlite_engine, text)
        msg = str(info.value).lower()
        assert "time" in msg, msg
        assert "sqlite" in msg, msg
        assert _data(data_calls) == []


class TestPostgresTryCast:
    @pytest.mark.parametrize("version", [(15, 4, 0), None])
    async def test_before_16_or_unknown_is_a_plain_cast(self, monkeypatch: pytest.MonkeyPatch, version) -> None:
        patch_profile(monkeypatch, version=version)
        plain = await emit(q(dimensions=[d("CAST(shifts.label AS TIME)")]), dialect="postgres")
        tried = await emit(q(dimensions=[d("TRY_CAST(shifts.label AS TIME)")]), dialect="postgres")
        assert tried == plain
        assert "PG_INPUT_IS_VALID(" not in tried.upper(), tried

    async def test_from_16_it_yields_null_for_invalid_text(self, monkeypatch: pytest.MonkeyPatch) -> None:
        patch_profile(monkeypatch, version=(16, 0, 0))
        tried = await emit(q(dimensions=[d("TRY_CAST(shifts.label AS TIME)")]), dialect="postgres")
        assert "PG_INPUT_IS_VALID(" in tried.upper(), tried
