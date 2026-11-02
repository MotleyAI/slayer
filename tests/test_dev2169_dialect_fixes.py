"""Per-dialect fixes: the ``collect`` fallback, Postgres list typing and type codes, ClickHouse array cells, SQL Server
JSON reads and ``split_part``, out-of-domain math (spec: aggregations/collect, queries/list-functions, queries/json,
queries/scalar-functions › Out-of-domain arguments yield NULL)."""

from __future__ import annotations

import contextlib
import re
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import ScalarArgumentError
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.sql.client import _map_type_code
from slayer.sql.dialects._clickhouse_cells import array_reader
from slayer.sql.dialects.sqlite import register_sqlite_udfs
from slayer.storage.sqlite_conn import open_connection
from tests._dev2120_fixtures import (
    NATIVE_LIST_DIALECTS,
    TIER1,
    emit_contractors,
    emit_lib,
    lib_engine,
    lib_query,
    patch_profile,
)
from tests._dev2137_fixtures import by, measure, query
from tests._engine_helpers import seeded_exec_engine


# --------------------------------------------------------------------------- #
# collect: the empty fallback takes the aggregate's own type
# --------------------------------------------------------------------------- #
_TYPED_EMPTY = (r"CAST\(\s*(ARRAY\s*)?\[\]\s+AS\b", r"ARRAY<\w+>\s*\[\]")


@pytest.mark.parametrize("dialect", NATIVE_LIST_DIALECTS)
@pytest.mark.parametrize("formula", ["collect(rate)", "collect(city)", "collect(skills.value)"])
async def test_collect_fallback_is_untyped(dialect: str, formula: str) -> None:
    sql = await emit_contractors(query(dimensions=["team"], measures=[measure(formula, "c")]), dialect=dialect)
    typed = [p for p in _TYPED_EMPTY if re.search(p, sql, re.I)]
    assert not typed, sql


UUID_A, UUID_B = "00000000-0000-0000-0000-00000000000a", "00000000-0000-0000-0000-00000000000b"


def _seed_amounts(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    with contextlib.closing(duckdb.connect(db_path)) as con:
        con.execute("CREATE TABLE amounts (id INTEGER, team VARCHAR, n DECIMAL(10, 2), big DECIMAL(38, 9), u UUID)")
        con.execute(f"INSERT INTO amounts VALUES (1, 'a', 1.5, 1.5, '{UUID_B}'), (2, 'a', 2.25, 2.25, '{UUID_A}'), "
                    f"(3, 'a', 1.5, 1.5, '{UUID_B}'), (4, 'b', NULL, NULL, NULL)")


def _amounts_model() -> SlayerModel:
    """``n`` / ``big`` are exact numerics read as DOUBLE; ``u`` a UUID read as TEXT, as ingestion types them."""
    return SlayerModel(name="amounts", sql_table="amounts", data_source="test", columns=[
        Column(name="id", type=DataType.INT, primary_key=True), Column(name="team", type=DataType.TEXT),
        Column(name="n", type=DataType.DOUBLE), Column(name="big", type=DataType.DOUBLE),
        Column(name="u", type=DataType.TEXT),
    ])


async def test_collect_over_exact_numerics_on_duckdb() -> None:
    async with seeded_exec_engine(dialect="duckdb", seed=_seed_amounts, models=[_amounts_model()]) as (engine, _db):
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "amounts", "dimensions": ["team"],
            "measures": [measure("collect(n)", "cn"), measure("collect(big)", "cbig")],
        }))
    for name in ("cn", "cbig"):
        got = {k: None if v is None else [float(x) for x in v] for k, v in by(resp, "team", name, model="amounts").items()}
        assert got == {"a": [1.5, 2.25], "b": []}, resp.sql


async def test_collect_over_uuid_on_duckdb() -> None:
    async with seeded_exec_engine(dialect="duckdb", seed=_seed_amounts, models=[_amounts_model()]) as (engine, _db):
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "amounts", "dimensions": ["team"], "measures": [measure("collect(u)", "cu")],
        }))
    got = {k: None if v is None else [str(x) for x in v] for k, v in by(resp, "team", "cu", model="amounts").items()}
    assert got == {"a": [UUID_A, UUID_B], "b": []}, resp.sql


# --------------------------------------------------------------------------- #
# Postgres: contains_any / contains_all compare canonical array types
# --------------------------------------------------------------------------- #
async def _postgres(expr: str) -> str:
    sql = await emit_contractors(query(dimensions=["id"], filters=[expr]), dialect="postgres")
    return " ".join(sql.upper().split())


@pytest.mark.parametrize(("expr", "array_type", "casts"), [
    ("contains_any(skills, ['go'])", "TEXT[]", 2),
    ("contains_all(skills, ['go', 'sql'])", "TEXT[]", 2),
    ("contains_any(skills, required)", "TEXT[]", 2),
    ("contains_all(scores, [1, 2])", "BIGINT[]", 2),
    ("contains_any(scores, [1, 9])", "BIGINT[]", 2),
])
async def test_postgres_list_operands_share_one_array_type(expr: str, array_type: str, casts: int) -> None:
    sql = await _postgres(expr)
    assert sql.count(f"AS {array_type})") >= casts, sql


# --------------------------------------------------------------------------- #
# Postgres array result type codes
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("oid", "pg_type"), [
    (1185, "timestamptz[]"), (1014, "bpchar[]"), (2951, "uuid[]"), (199, "json[]"), (3807, "jsonb[]"),
    (1001, "bytea[]"),
])
def test_postgres_array_type_codes_are_lists(oid: int, pg_type: str) -> None:
    assert _map_type_code(oid, "postgresql") == "list", pg_type


# --------------------------------------------------------------------------- #
# ClickHouse array cells keep every escape
# --------------------------------------------------------------------------- #
_CH_ESCAPES = {"\\": "\\\\", "'": "\\'", "\n": "\\n", "\t": "\\t", "\r": "\\r", "\0": "\\0", "\b": "\\b",
               "\f": "\\f"}


def _clickhouse_cell(values: list[Optional[str]]) -> str:
    """``values`` as the ClickHouse HTTP driver returns an ``Array(Nullable(String))`` cell."""
    def quoted(value: str) -> str:
        return "'" + "".join(_CH_ESCAPES.get(ch, ch) for ch in value) + "'"
    return "[" + ",".join("NULL" if v is None else quoted(v) for v in values) + "]"


_STRINGS = array_reader("Array(Nullable(String))")


@pytest.mark.parametrize("values", [
    ["back\\slash", "quo'te", "new\nline", "tab\tbed", "carriage\rreturn", "nul\0byte", "bell\bspace", "form\ffeed"],
    ["c:\\path\\to\\file.txt", "\\\\server\\share"],
    ["\\\\", "trailing\\", "\\x41", "\\N", "a,b", "[x]", "é€\U0001F600"],
    ["esc\x1bape", "vt\vab", "al\aert", "\x01\x7f"],
    [None, "", "x", None],
])
def test_clickhouse_cell_round_trip(values: list[Optional[str]]) -> None:
    assert _STRINGS is not None
    assert _STRINGS(_clickhouse_cell(values)) == values


@pytest.mark.parametrize(("cell", "expected"), [
    (r"['\x41\x62']", ["Ab"]),
    (r"['\a\v\e']", ["\a\v\x1b"]),
    (r"['say \"hi\"']", ['say "hi"']),
    # An unknown escape keeps its backslash, as ClickHouse's own parser does ('a\qb' is four characters there).
    (r"['a\qb']", ["a\\qb"]),
    (r"['a\xZ1b']", ["a\\xZ1b"]),
    (r"['a\x']", ["a\\x"]),
    (r"['a\x4']", ["a\\x4"]),
])
def test_clickhouse_cell_escapes(cell: str, expected: list[str]) -> None:
    assert _STRINGS is not None
    assert _STRINGS(cell) == expected


# --------------------------------------------------------------------------- #
# SQL Server: long JSON scalars fail loud on 2022+; split_part has no 4,000-character limit
# --------------------------------------------------------------------------- #
async def _tsql(monkeypatch, expr: str, version: Optional[tuple[int, ...]]) -> str:
    patch_profile(monkeypatch, version=version)
    return " ".join((await emit_lib(lib_query(expr), dialect="tsql")).split())


@pytest.mark.parametrize("version", [(16, 0, 0), (17, 0, 0), None])
@pytest.mark.parametrize("expr", ["json_value(payload, '$.a.b')", "payload ->> 'ok'",
                                  "json_value(payload, '$.qty' RETURNING INT)"])
async def test_tsql_json_value_rereads_strict(monkeypatch, expr: str, version: Any) -> None:
    sql = await _tsql(monkeypatch, expr, version)
    assert "'strict $[0]." in sql, sql
    assert "JSON_PATH_EXISTS(" in sql.upper(), sql


async def test_tsql_json_value_before_2022_stays_lax(monkeypatch) -> None:
    sql = await _tsql(monkeypatch, "json_value(payload, '$.a.b')", (15, 0, 0))
    assert "strict" not in sql, sql
    assert "VALUE)" not in sql.upper(), sql


@pytest.mark.parametrize("version", [(16, 0, 0), None])
async def test_tsql_json_value_takes_a_scalar_document(monkeypatch, version: Any) -> None:
    sql = await _tsql(monkeypatch, "json_value(payload, '$')", version)
    assert re.search(r"ISJSON\(\s*lib\.payload\s*,\s*VALUE\s*\)", sql, re.I), sql


@pytest.mark.parametrize("version", [(16, 0, 0), None])
async def test_tsql_json_value_reads_the_document_wrapped(monkeypatch, version: Any) -> None:
    # JSON_VALUE / JSON_QUERY refuse a scalar document even where ISJSON(…, VALUE) accepts it.
    sql = await _tsql(monkeypatch, "json_value(payload, '$')", version)
    assert "JSON_VALUE(CONCAT('[', CAST(lib.payload AS NVARCHAR(MAX)), ']'), '$[0]')" in sql, sql
    assert "JSON_VALUE(lib.payload" not in sql, sql


@pytest.mark.parametrize("expr", ["split_part(s, '/', 2)", "split_part(s, '/', -1)", "split_part(s, tag, 3)"])
async def test_tsql_split_part_reads_no_json(monkeypatch, expr: str) -> None:
    sql = await _tsql(monkeypatch, expr, None)
    assert "JSON_VALUE" not in sql.upper(), sql
    assert "CHARINDEX(" in sql.upper(), sql


# --------------------------------------------------------------------------- #
# Out-of-domain math: literals fail at plan time, the SQLite functions yield NULL
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
async def sqlite_engine():
    async with lib_engine("sqlite") as e:
        yield e


@pytest.mark.parametrize(("expr", "function", "arguments"), [
    ("ln(0)", "ln", ["0"]), ("ln(-1)", "ln", ["-1"]), ("log10(0)", "log10", ["0"]), ("log2(-2)", "log2", ["-2"]),
    ("log(0)", "log", ["0"]), ("log(1, x)", "log", ["1"]), ("log(-2, x)", "log", ["-2"]), ("log(2, 0)", "log", ["0"]),
    ("sqrt(-1)", "sqrt", ["-1"]), ("pow(-8, 0.5)", "pow", ["-8", "0.5"]), ("power(-8, 0.5)", "power", ["-8", "0.5"]),
    ("pow(0, -1)", "pow", ["0", "-1"]),
])
async def test_literal_out_of_domain_rejected(sqlite_engine, expr: str, function: str, arguments: list[str]) -> None:
    query = lib_query(expr)
    with pytest.raises(ScalarArgumentError) as info:
        await sqlite_engine.execute(query)
    msg = str(info.value)
    assert f"{function}(" in msg, msg
    assert any(a in msg for a in arguments), msg


@pytest.mark.parametrize("expr", ["ln(1)", "log(2, 8)", "log(10, x)", "sqrt(0)", "pow(-8, 3)", "pow(0, 0)",
                                  "pow(4, 0.5)", "log2(x)"])
async def test_literal_in_domain_accepted(sqlite_engine, expr: str) -> None:
    await sqlite_engine.execute(lib_query(expr))


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("expr", ["ln(0)", "sqrt(-1)", "log(1, x)", "pow(-8, 0.5)"])
async def test_literal_rejected_before_emission(dialect: str, expr: str) -> None:
    query = lib_query(expr)
    with pytest.raises(ScalarArgumentError):
        await emit_lib(query, dialect=dialect)


@pytest.mark.parametrize("call", [
    "ln(0)", "ln(-1)", "log10(0)", "log10(-1)", "log2(0)", "log2(-1)", "log(1, 8)", "log(0, 8)", "log(-2, 8)",
    "log(2, 0)", "log(2, -1)", "sqrt(-1)", "pow(0, -1)", "pow(-2, 0.5)", "power(-2, 0.5)",
])
def test_sqlite_math_out_of_domain_is_null(call: str) -> None:
    with open_connection(":memory:") as conn:
        register_sqlite_udfs(conn)
        assert conn.execute(f"SELECT {call}").fetchone()[0] is None
