"""CAST / ``::`` / TRY_CAST in Mode-B expressions (spec: queries/casts)."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.errors import QueryTypeError
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import SlayerSQLClient
from slayer.sql.dialects import get_dialect
from slayer.sql.dialects.base import ServerProfile
from tests._dev2101_fixtures import (
    EXEC_BACKENDS, TIER1, by_key, day, emit, engine_for, m, make_exec_engine, num, q, single,
)

TRY_CODE = {1: 12, 2: None, 3: None, 4: 7, 5: None}


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


def _ints(values: dict) -> dict[int, Any]:
    return {int(k): (None if v is None else int(v)) for k, v in values.items()}


def _casts(sql: str, *, dialect: str) -> list[exp.Cast]:
    return list(sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Cast))


class TestCastExecution:
    @pytest.mark.parametrize("spelling", ["CAST(amount AS DECIMAL(10,2))", "amount::decimal(10,2)"])
    async def test_parameterised_cast_in_measure(self, engine: SlayerQueryEngine, spelling: str) -> None:
        assert num(single(await engine.execute(q(measures=[m(f"sum({spelling})")])))) == pytest.approx(100.0)

    async def test_chained_casts(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id", {"expression": "amount::int::text", "name": "x"}]))
        assert {int(k): v for k, v in by_key(resp, "id", "x").items()} == {1: "10", 2: "20", 3: "30", 4: "40", 5: None}

    @pytest.mark.parametrize("position", ["dimension", "filter", "order"])
    async def test_cast_in_every_position(self, engine: SlayerQueryEngine, position: str) -> None:
        expr = "CAST(quantity AS TEXT)"
        if position == "dimension":
            resp = await engine.execute(q(dimensions=["id", {"expression": expr, "name": "x"}]))
            assert by_key(resp, "id", "x")[3] == "3"
        elif position == "filter":
            resp = await engine.execute(q(dimensions=["id"], filters=[f"{expr} = '4'"]))
            assert [int(r["orders.id"]) for r in resp.data] == [5]
        else:
            resp = await engine.execute(q(dimensions=["id"], distinct_dimension_values=False,
                                          order=[{"column": expr, "direction": "desc"}, {"column": "id"}]))
            assert [int(r["orders.id"]) for r in resp.data] == [5, 3, 2, 4, 1]

    async def test_cast_to_date_is_a_date_operand(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=[
            "id", {"expression": "date_diff('day', CAST(created_txt AS DATE), DATE '2025-01-01')", "name": "x"},
        ]))
        expected = {1: date(2024, 12, 25), 2: date(2024, 6, 1), 3: date(2024, 2, 29), 4: date(2023, 1, 1), 5: None}
        assert _ints(by_key(resp, "id", "x")) == {
            k: (None if v is None else (date(2025, 1, 1) - v).days) for k, v in expected.items()
        }


@pytest.fixture
async def sqlite_engine():
    async for e in engine_for("sqlite"):
        yield e


class TestInvalidInputFollowsTheDatabase:
    async def test_sqlite_cast_keeps_its_own_semantics(self, sqlite_engine: SlayerQueryEngine) -> None:
        resp = await sqlite_engine.execute(q(dimensions=["id", {"expression": "CAST(code AS INT)", "name": "x"}]))
        assert _ints(by_key(resp, "id", "x")) == {1: 12, 2: 0, 3: 12, 4: 7, 5: None}


class TestTryCastExecution:
    async def test_invalid_text_becomes_null(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id", {"expression": "TRY_CAST(code AS INT)", "name": "x"}]))
        assert _ints(by_key(resp, "id", "x")) == TRY_CODE

    async def test_prefix_is_not_truncated(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], filters=["TRY_CAST(code AS INT) IS NULL"]))
        assert {int(r["orders.id"]) for r in resp.data} == {2, 3, 5}

    async def test_try_cast_order_target(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(dimensions=["id"], distinct_dimension_values=False,
                                      filters=["TRY_CAST(code AS INT) IS NOT NULL"],
                                      order=[{"column": "TRY_CAST(code AS INT)", "direction": "desc"}]))
        assert [int(r["orders.id"]) for r in resp.data] == [1, 4]

    async def test_try_cast_in_measure_and_filter(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(q(measures=[m("sum(TRY_CAST(code AS INT))")],
                                      filters=["TRY_CAST(code AS INT) > 0"]))
        assert num(single(resp)) == 19

    @pytest.mark.parametrize(("text", "expected"), [
        ("12", 12.0), (" 12 ", 12.0), ("+3", 3.0), ("-4.5e1", -45.0), ("2.5", 2.5),
        ("abc", None), ("12abc", None), ("1e", None), ("", None), ("1 2", None),
    ])
    async def test_number_grammar(self, engine: SlayerQueryEngine, text: str, expected: float | None) -> None:
        resp = await engine.execute(q(measures=[m(f"max(TRY_CAST('{text}' AS DOUBLE))")]))
        assert num(single(resp)) == expected

    @pytest.mark.parametrize(("text", "expected"), [
        ("2024-02-29", "2024-02-29"), ("2024-02-30", None), ("2024-13-01", None), ("not a date", None),
    ])
    async def test_date_grammar(self, engine: SlayerQueryEngine, text: str, expected: str | None) -> None:
        resp = await engine.execute(q(measures=[m(f"max(TRY_CAST('{text}' AS DATE))")]))
        assert day(single(resp)) == expected


class TestSqliteTryCastBooleans:
    @pytest.mark.parametrize(("text", "expected"), [
        ("true", True), ("FALSE", False), ("t", True), ("F", False), ("yes", True), ("No", False),
        ("1", True), ("0", False), ("maybe", None), ("2", None),
    ])
    async def test_boolean_words(self, sqlite_engine: SlayerQueryEngine, text: str, expected) -> None:
        resp = await sqlite_engine.execute(q(measures=[m(f"max(TRY_CAST('{text}' AS BOOLEAN))")]))
        value = single(resp)
        assert (None if value is None else bool(value)) == expected


class TestCastEmission:
    async def test_postgres_keeps_the_parameterised_type(self) -> None:
        sql = await emit(q(measures=[m("sum(CAST(amount AS DECIMAL(10,2)))")]), dialect="postgres")
        assert "DECIMAL(10, 2)" in sql, sql

    async def test_mysql_text_cast_uses_its_character_type(self) -> None:
        sql = await emit(q(dimensions=["id", {"expression": "amount::text", "name": "x"}]), dialect="mysql")
        casts = [c for c in _casts(sql, dialect="mysql") if "amount" in c.this.sql().lower()]
        assert casts, sql
        assert all("CHAR" in c.to.sql(dialect="mysql").upper() for c in casts), sql

    async def test_chained_casts_nest(self) -> None:
        sql = await emit(q(dimensions=["id", {"expression": "amount::int::text", "name": "x"}]), dialect="postgres")
        outer = next(c for c in _casts(sql, dialect="postgres") if isinstance(c.this, exp.Cast))
        assert outer.to.is_type(exp.DataType.Type.TEXT), sql
        assert outer.this.to.is_type(exp.DataType.Type.INT), sql

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_cast_emits_everywhere(self, dialect: str) -> None:
        await emit(q(dimensions=["id", {"expression": "CAST(quantity AS TEXT)", "name": "x"}],
                     measures=[m("sum(amount::int)")], filters=["CAST(amount AS DECIMAL(10,2)) > 1"]),
                   dialect=dialect)

    async def test_unknown_target_type_renders(self) -> None:
        sql = await emit(q(dimensions=["id", {"expression": "CAST(payload AS JSON)", "name": "x"}]), dialect="postgres")
        assert any(c.to.is_type(exp.DataType.Type.JSON) for c in _casts(sql, dialect="postgres")), sql

    @pytest.mark.parametrize("dialect", ["postgres", "tsql"])
    async def test_dialect_specific_type_accepted_where_expressible(self, dialect: str) -> None:
        sql = await emit(q(measures=[m("sum(CAST(amount AS MONEY))")]), dialect=dialect)
        assert "MONEY" in sql.upper(), sql

    @pytest.mark.parametrize("dialect", ["bigquery", "mysql", "trino", "snowflake", "clickhouse"])
    async def test_inexpressible_type_rejected_at_plan_time(self, dialect: str) -> None:
        query = q(measures=[m("sum(CAST(amount AS MONEY))")])
        with pytest.raises(QueryTypeError) as info:
            await emit(query, dialect=dialect)
        msg = str(info.value).lower()
        assert "money" in msg, msg
        assert dialect in msg, msg


_TRY_CAST_MARKERS = {
    "duckdb": "TRY_CAST(",
    "tsql": "TRY_CAST(",
    "snowflake": "TRY_CAST(",
    "trino": "TRY_CAST(",
    "bigquery": "SAFE_CAST(",
    "clickhouse": "ACCURATECASTORNULL(",
    "sqlite": "SLAYER_TRY_CAST(",
    "mysql": "REGEXP",
}


class TestTryCastEmission:
    @pytest.mark.parametrize("dialect", sorted(_TRY_CAST_MARKERS))
    async def test_per_dialect_form(self, dialect: str) -> None:
        sql = await emit(q(dimensions=["id", {"expression": "TRY_CAST(code AS INT)", "name": "x"}]), dialect=dialect)
        assert _TRY_CAST_MARKERS[dialect] in sql.upper(), sql

    @pytest.mark.parametrize("target", ["INT", "DOUBLE", "DATE", "TIMESTAMP", "BOOLEAN", "DECIMAL(10,2)"])
    @pytest.mark.parametrize("dialect", TIER1)
    async def test_every_target_emits(self, dialect: str, target: str) -> None:
        await emit(q(dimensions=["id", {"expression": f"TRY_CAST(code AS {target})", "name": "x"}]), dialect=dialect)


def _patch_profile(monkeypatch, *, version: tuple[int, int] | None = None, fail: bool = False) -> list[int]:
    calls: list[int] = []

    async def fake(_self) -> ServerProfile:
        calls.append(1)
        if fail:
            raise ConnectionError("probe failed")
        return ServerProfile(version=version)

    monkeypatch.setattr(SlayerSQLClient, "server_profile", fake)
    return calls


TRY_Q = q(dimensions=["id", {"expression": "TRY_CAST(code AS INT)", "name": "x"}])


class TestPostgresTryCast:
    def test_profile_probe_statement(self) -> None:
        assert get_dialect("postgres").server_profile_sql() == "SELECT current_setting('server_version_num')"

    @pytest.mark.parametrize(("row", "version"), [
        (("160002",), (16, 2)), (("150004",), (15, 4)), ((170000,), (17, 0)), (("garbage",), None), ((None,), None),
    ])
    def test_profile_parse(self, row: tuple, version: tuple[int, int] | None) -> None:
        assert get_dialect("postgres").parse_server_profile(base_row=row) == ServerProfile(version=version)

    def test_profile_parse_tolerates_no_row(self) -> None:
        assert get_dialect("postgres").parse_server_profile(base_row=None) == ServerProfile()

    async def test_postgres_16_guards_with_the_native_check(self, monkeypatch) -> None:
        _patch_profile(monkeypatch, version=(16, 2))
        sql = await emit(TRY_Q, dialect="postgres")
        assert "PG_INPUT_IS_VALID(" in sql.upper(), sql

    @pytest.mark.parametrize("version", [(15, 4), None])
    async def test_older_or_unknown_server_gets_a_plain_cast(self, monkeypatch, version) -> None:
        _patch_profile(monkeypatch, version=version)
        sql = await emit(TRY_Q, dialect="postgres")
        assert "PG_INPUT_IS_VALID(" not in sql.upper(), sql
        assert "TRY_CAST" not in sql.upper(), sql
        assert _casts(sql, dialect="postgres"), sql

    async def test_failed_probe_gets_a_plain_cast(self, monkeypatch) -> None:
        _patch_profile(monkeypatch, fail=True)
        sql = await emit(TRY_Q, dialect="postgres")
        assert "PG_INPUT_IS_VALID(" not in sql.upper(), sql

    async def test_profile_probed_only_for_try_cast(self, monkeypatch) -> None:
        calls = _patch_profile(monkeypatch, version=(16, 2))
        await emit(q(dimensions=["id", {"expression": "CAST(code AS INT)", "name": "x"}]), dialect="postgres")
        assert calls == []
        await emit(TRY_Q, dialect="postgres")
        assert calls == [1]
