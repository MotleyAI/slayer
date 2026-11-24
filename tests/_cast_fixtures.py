"""Shared fixtures for the cast type grammar, array casts, cast values and the TRY_CAST integer rule (spec: queries/casts).

Underscore-prefixed so pytest skips collection.

tags (id, codes): 1 [1, 3] | 2 [2] | 3 NULL — ``codes`` is TEXT in each database's own array-text form.
"""

from __future__ import annotations

import math
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction

from tests._engine_helpers import seeded_exec_engine

#: Each database's text spelling of an INT array.
ARRAY_TEXT = {"postgres": ("{1,3}", "{2}"), "duckdb": ("[1, 3]", "[2]")}
CONTAINS_3 = frozenset({1})
LENGTHS = {1: 2, 2: 1, 3: None}


def tags_dataset(*, data_source: str = "test") -> SlayerDataset:
    return SlayerDataset(
        name="tags", sql_table="tags", data_source=data_source,
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True), DatasetField(name="codes", datatype=DataType.STRING)],
    )


def tags_statements(dialect: str) -> list[str]:
    first, second = ARRAY_TEXT[dialect]
    return [
        "CREATE TABLE tags (id INTEGER PRIMARY KEY, codes TEXT)",
        f"INSERT INTO tags VALUES (1, '{first}'), (2, '{second}'), (3, NULL)",
    ]


def _seed_duckdb_tags(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        for stmt in tags_statements("duckdb"):
            con.execute(stmt)
    finally:
        con.close()


async def duckdb_tags_engine() -> AsyncIterator[SlayerQueryEngine]:
    pytest.importorskip("duckdb")
    async with seeded_exec_engine(dialect="duckdb", seed=_seed_duckdb_tags, datasets=[tags_dataset()]) as (engine, _):
        yield engine


def tags_query(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"population": "tags", **kw})


async def check_array_cast(engine: SlayerQueryEngine) -> None:
    """``contains`` / ``length`` over ``CAST(codes AS INT[])`` read it as a list of INT."""
    resp = await engine.execute(tags_query(dimensions=["tags.id"], filter=["contains(CAST(tags.codes AS INT[]), 3)"]))
    assert {int(r["tags.id"]) for r in resp.data} == CONTAINS_3, resp.sql
    resp = await engine.execute(tags_query(dimensions=["tags.id", {"field": "length(tags.codes::INT[])", "alias": "n"}]))
    got = {int(r["tags.id"]): (None if r["n"] is None else int(r["n"])) for r in resp.data}
    assert got == LENGTHS, resp.sql


# --------------------------------------------------------------------------- #
# TRY_CAST to an integer type: integer text or a number rounded half away from zero, else NULL
# --------------------------------------------------------------------------- #
_HUGE = "1" + "0" * 40
#: (input expression, target type, expected value).
TRY_CAST_TARGET_CASES: list[tuple[str, str, Any]] = [
    ("'12'", "INT", 12),
    ("'-12'", "INT", -12),
    ("' 7 '", "INT", 7),
    ("'+3'", "INT", 3),
    ("'1.5'", "INT", None),
    ("'1e3'", "INT", None),
    ("'2.0'", "INT", None),
    ("'12abc'", "INT", None),
    ("'abc'", "INT", None),
    ("''", "INT", None),
    ("'1 2'", "INT", None),
    ("CAST(1.5 AS DOUBLE)", "INT", 2),
    ("CAST(2 AS DOUBLE)", "INT", 2),
    ("CAST(-3 AS DOUBLE)", "INT", -3),
    ("1.5", "INT", 2),
    ("'99999'", "SMALLINT", None),
    ("'32767'", "SMALLINT", 32767),
    ("'-32768'", "SMALLINT", -32768),
    ("'32768'", "SMALLINT", None),
    ("CAST(32767 AS DOUBLE)", "SMALLINT", 32767),
    ("CAST(32768 AS DOUBLE)", "SMALLINT", None),
    ("CAST(-32769 AS DOUBLE)", "SMALLINT", None),
    (f"'{_HUGE}'", "BIGINT", None),
    ("'9223372036854775807'", "BIGINT", 9223372036854775807),
    ("CAST(1e19 AS DOUBLE)", "BIGINT", None),
    ("CAST('9223372036854775807' AS BIGINT)", "BIGINT", 9223372036854775807),
    ("CAST('-9223372036854775808' AS BIGINT)", "BIGINT", -9223372036854775808),
    ("1 > 0", "INT", 1),
    ("1e3", "INT", 1000),
    ("1 > 2", "DOUBLE", 0.0),
    ("'1.5'", "DOUBLE", 1.5),
    ("'1e3'", "DOUBLE", 1000.0),
    # A JSON number is a number, a JSON string text.
    ("json_value('{{\"v\": 1.5}}', '$.v')", "INT", 2),
    ("json_value('{{\"v\": -2.5}}', '$.v')", "SMALLINT", -3),
    ("json_value('{{\"v\": \"1.5\"}}', '$.v')", "INT", None),
    ("json_value('{{\"v\": 7}}', '$.v')", "INT", 7),
]
TRY_CAST_CASE_IDS = [f"{src}->{to}" for src, to, _ in TRY_CAST_TARGET_CASES]


def try_cast_metrics() -> list[dict[str, str]]:
    return [{"metric": f"max(TRY_CAST({src} AS {to}))", "alias": f"c{i}"}
            for i, (src, to, _) in enumerate(TRY_CAST_TARGET_CASES)]


def _normalised(value: Any, *, target: str) -> Any:
    if value is None:
        return None
    return float(value) if target == "DOUBLE" else int(value)


async def check_try_cast_targets(engine: SlayerQueryEngine, *, dataset: str) -> None:
    """Every ``TRY_CAST_TARGET_CASES`` row in one query over any non-empty ``dataset``."""
    resp = await engine.execute(SlayerQuery.model_validate({"population": dataset, "metrics": try_cast_metrics()}))
    assert len(resp.data) == 1, resp.data
    row = resp.data[0]
    got = {TRY_CAST_CASE_IDS[i]: _normalised(row[f"c{i}"], target=to)
           for i, (_, to, _) in enumerate(TRY_CAST_TARGET_CASES)}
    want = {TRY_CAST_CASE_IDS[i]: expected for i, (_, _, expected) in enumerate(TRY_CAST_TARGET_CASES)}
    assert got == want, resp.sql


# --------------------------------------------------------------------------- #
# CAST / TRY_CAST have one meaning on every dialect (constant inputs)
# --------------------------------------------------------------------------- #
def as_bool(cast: str) -> str:
    """``cast`` read through ``= TRUE`` / ``= FALSE``: 1, 0 or NULL."""
    return f"CASE WHEN {cast} = TRUE THEN 1 WHEN {cast} = FALSE THEN 0 END"


class CastCase(BaseModel):
    """A constant Mode-B expression and its value on every Tier-1 dialect."""

    model_config = ConfigDict(frozen=True)
    expr: str
    expected: Any
    #: Needs exact DECIMAL arithmetic beyond a double (not SQLite).
    exact: bool = False


def _c(expr: str, expected: Any, *, exact: bool = False) -> CastCase:
    return CastCase(expr=expr, expected=expected, exact=exact)


AUTHOR_CAST_CASES: list[CastCase] = [
    _c(as_bool("CAST(5 AS BOOLEAN)"), 1),
    _c(as_bool("CAST(-1 AS BOOLEAN)"), 1),
    _c(as_bool("CAST(0 AS BOOLEAN)"), 0),
    _c(as_bool("CAST(0.5 AS BOOLEAN)"), 1),
    _c(as_bool("CAST(0.0 AS BOOLEAN)"), 0),
    _c(as_bool("CAST(CAST(NULL AS INT) AS BOOLEAN)"), None),
    _c(as_bool("CAST(1 > 0 AS BOOLEAN)"), 1),
    _c("CAST(2.5 AS INT)", 3),
    _c("CAST(-2.5 AS INT)", -3),
    _c("CAST(2.4 AS INT)", 2),
    _c("CAST(2.675 AS INT)", 3),
    _c("CAST(CAST(2.6 AS DOUBLE) AS INT)", 3),
    _c("CAST(CAST(-2.6 AS DOUBLE) AS INTEGER)", -3),
    _c("CAST(7.5 AS SMALLINT)", 8),
    _c("CAST(-7.5 AS BIGINT)", -8),
    _c("CAST(12.345 AS DECIMAL(10,2))", 12.35),
    _c("CAST('12.345' AS DECIMAL(10,2))", 12.35),
    _c("CAST(-12.345 AS NUMERIC(10,2))", -12.35),
    _c("CAST(' 1.2345e1 ' AS DECIMAL(10,2))", 12.35),
    _c("CAST(12.5 AS DECIMAL)", 13),
    _c("CAST(12.4 AS NUMERIC)", 12),
    _c("CAST(-12.5 AS DECIMAL)", -13),
    _c("CAST(12.5 AS DECIMAL(5))", 13),
    _c("CAST('abcdef' AS VARCHAR(3))", "abc"),
    _c("CAST('ñandú' AS VARCHAR(2))", "ña"),
    _c("CAST('ab' AS VARCHAR(3))", "ab"),
    _c("CAST('ab   ' AS VARCHAR(3))", "ab "),
    _c("CAST(12345 AS VARCHAR(3))", "123"),
    _c("CAST(CAST(NULL AS TEXT) AS VARCHAR(3))", None),
    _c("CAST('abcdef' AS STRING(3))", "abc"),
]

TRY_CAST_ROUNDING_CASES: list[CastCase] = [
    _c("TRY_CAST(2.5 AS INT)", 3),
    _c("TRY_CAST(-2.5 AS INT)", -3),
    _c("TRY_CAST(CAST(2.6 AS DOUBLE) AS INT)", 3),
    _c("TRY_CAST(CAST(32766.5 AS DECIMAL(7,1)) AS SMALLINT)", 32767),
    _c("TRY_CAST(CAST(32767.5 AS DECIMAL(7,1)) AS SMALLINT)", None),
    _c("TRY_CAST(CAST(-32767.5 AS DECIMAL(7,1)) AS SMALLINT)", -32768),
    _c("TRY_CAST(CAST(-32768.5 AS DECIMAL(7,1)) AS SMALLINT)", None),
    _c("TRY_CAST(CAST(2147483646.5 AS DECIMAL(12,1)) AS INT)", 2147483647),
    _c("TRY_CAST(CAST(2147483647.5 AS DECIMAL(12,1)) AS INT)", None),
    _c("TRY_CAST(CAST(-2147483647.5 AS DECIMAL(12,1)) AS INT)", -2147483648),
    _c("TRY_CAST(CAST(-2147483648.5 AS DECIMAL(12,1)) AS INT)", None),
    _c("TRY_CAST(CAST(9223372036854775806.5 AS DECIMAL(20,1)) AS BIGINT)", 9223372036854775807, exact=True),
    _c("TRY_CAST(CAST(9223372036854775807.5 AS DECIMAL(20,1)) AS BIGINT)", None, exact=True),
    _c("TRY_CAST(CAST(-9223372036854775807.5 AS DECIMAL(20,1)) AS BIGINT)", -9223372036854775808, exact=True),
    _c("TRY_CAST(CAST(-9223372036854775808.5 AS DECIMAL(20,1)) AS BIGINT)", None, exact=True),
    _c("TRY_CAST(CAST(9223372036854775807 AS DOUBLE) AS BIGINT)", None),
    _c("TRY_CAST(CAST(-9223372036854775808 AS DOUBLE) AS BIGINT)", -9223372036854775808),
    _c(as_bool("TRY_CAST(5 AS BOOLEAN)"), 1),
    _c(as_bool("TRY_CAST(0 AS BOOLEAN)"), 0),
    _c(as_bool("TRY_CAST(CAST(NULL AS INT) AS BOOLEAN)"), None),
    _c("TRY_CAST(12.345 AS DECIMAL(10,2))", 12.35),
    _c("TRY_CAST('12.345' AS DECIMAL(10,2))", 12.35),
    _c("TRY_CAST(12345.6 AS DECIMAL(3,1))", None),
]
CAST_MATRIX = [*AUTHOR_CAST_CASES, *TRY_CAST_ROUNDING_CASES]
CAST_MATRIX_IDS = [c.expr for c in CAST_MATRIX]


def assert_cast_value(actual: Any, expected: Any, *, where: str) -> None:
    if expected is None:
        assert actual is None, f"{where}: expected NULL, got {actual!r}"
        return
    assert actual is not None, f"{where}: expected {expected!r}, got NULL"
    if isinstance(expected, str):
        assert actual == expected, f"{where}: expected {expected!r}, got {actual!r}"
    elif isinstance(expected, int):
        assert not isinstance(actual, str), f"{where}: expected a number, got {actual!r}"
        assert Decimal(str(actual)) == Decimal(expected), f"{where}: expected {expected}, got {actual!r}"
    else:
        assert not isinstance(actual, str), f"{where}: expected a number, got {actual!r}"
        assert math.isclose(float(actual), expected, abs_tol=1e-9), f"{where}: expected {expected}, got {actual!r}"


async def check_author_casts(engine: SlayerQueryEngine, *, dataset: str, exact_decimals: bool = True) -> None:
    """Every ``CAST_MATRIX`` row as ``max(expr)`` in one query over any non-empty ``dataset``."""
    cases = [c for c in CAST_MATRIX if exact_decimals or not c.exact]
    metrics = [{"metric": f"max({c.expr})", "alias": f"c{i}"} for i, c in enumerate(cases)]
    resp = await engine.execute(SlayerQuery.model_validate({"population": dataset, "metrics": metrics}))
    assert len(resp.data) == 1, resp.data
    problems = []
    for i, case in enumerate(cases):
        try:
            assert_cast_value(resp.data[0][f"c{i}"], case.expected, where=case.expr)
        except AssertionError as exc:
            problems.append(str(exc))
    assert not problems, "\n".join(problems) + f"\nSQL:\n{resp.sql}"


# --------------------------------------------------------------------------- #
# casts (id, qty, d, s, r): 1 0 2.5 'abcdef' 1.5 | 2 5 -2.5 'ñandú' 2.0 | 3 -1 2.4 'ab' -2.6 | 4 NULL 2.675 NULL NULL
# --------------------------------------------------------------------------- #
CASTS_ROWS: list[tuple[Any, ...]] = [
    (1, 0, 2.5, "abcdef", 1.5), (2, 5, -2.5, "ñandú", 2.0), (3, -1, 2.4, "ab", -2.6), (4, None, 2.675, None, None),
]
#: Per-dialect (decimal, double, text) column types.
_CASTS_TYPES = {
    "sqlite": ("DECIMAL(10,3)", "REAL", "TEXT"),
    "duckdb": ("DECIMAL(10,3)", "DOUBLE", "VARCHAR"),
    "postgres": ("NUMERIC(10,3)", "DOUBLE PRECISION", "TEXT"),
}
#: (expression over ``casts``, value per id); BOOLEAN values as 1 / 0.
CASTS_COLUMN_CASES: list[tuple[str, dict[int, Any]]] = [
    ("CAST(casts.qty AS BOOLEAN)", {1: 0, 2: 1, 3: 1, 4: None}),
    ("TRY_CAST(casts.qty AS BOOLEAN)", {1: 0, 2: 1, 3: 1, 4: None}),
    ("CAST(casts.d AS INT)", {1: 3, 2: -3, 3: 2, 4: 3}),
    ("CAST(casts.s AS VARCHAR(2))", {1: "ab", 2: "ña", 3: "ab", 4: None}),
    ("TRY_CAST(casts.r AS INT)", {1: 2, 2: 2, 3: -3, 4: None}),
]


def casts_dataset(*, data_source: str = "test") -> SlayerDataset:
    return SlayerDataset(
        name="casts", sql_table="casts", data_source=data_source,
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="qty", datatype=DataType.INTEGER),
            DatasetField(name="d", datatype=DataType.FLOAT, db_type="DECIMAL(10,3)"),
            DatasetField(name="s", datatype=DataType.STRING),
            DatasetField(name="r", datatype=DataType.FLOAT),
        ],
    )


def casts_statements(dialect: str) -> list[str]:
    decimal, double, text = _CASTS_TYPES[dialect]

    def lit(v: Any) -> str:
        return "NULL" if v is None else f"'{v}'" if isinstance(v, str) else str(v)

    values = ", ".join("(" + ", ".join(lit(v) for v in row) + ")" for row in CASTS_ROWS)
    return [f"CREATE TABLE casts (id INTEGER PRIMARY KEY, qty INTEGER, d {decimal}, s {text}, r {double})",
            f"INSERT INTO casts VALUES {values}"]


def _seed_casts(dialect: str, db_path: str) -> None:
    if dialect == "sqlite":
        with transaction(db_path) as con:
            for stmt in casts_statements("sqlite"):
                con.execute(stmt)
        return
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    try:
        for stmt in casts_statements("duckdb"):
            con.execute(stmt)
    finally:
        con.close()


async def casts_engine(dialect: str) -> AsyncIterator[SlayerQueryEngine]:
    """An executing engine over ``casts`` on ``sqlite`` or ``duckdb`` (fixture body)."""
    async with seeded_exec_engine(dialect=dialect, seed=lambda p: _seed_casts(dialect, p),
                                  datasets=[casts_dataset()]) as (engine, _):
        yield engine


async def check_casts_columns(engine: SlayerQueryEngine) -> None:
    """Every ``CASTS_COLUMN_CASES`` row, and ``CAST(qty AS BOOLEAN) = TRUE`` as a filter."""
    for expr, expected in CASTS_COLUMN_CASES:
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": "casts", "dimensions": ["casts.id", {"field": expr, "alias": "v"}],
        }))
        got = {int(r["casts.id"]): r["v"] for r in resp.data}
        assert set(got) == set(expected), (expr, resp.sql)
        for rid, want in expected.items():
            cell = boolean_cell(got[rid]) if "BOOLEAN" in expr else got[rid]
            assert_cast_value(cell, want, where=f"{expr} id={rid}\n{resp.sql}")
    resp = await engine.execute(SlayerQuery.model_validate({
        "population": "casts", "dimensions": ["casts.id"], "filter": ["CAST(casts.qty AS BOOLEAN) = TRUE"],
    }))
    assert {int(r["casts.id"]) for r in resp.data} == {2, 3}, resp.sql


# --------------------------------------------------------------------------- #
# A number cast to BOOLEAN over a column: ``id - 3`` is true exactly when ``id <> 3``
# --------------------------------------------------------------------------- #
def boolean_cell(value: Any) -> int | None:
    """A boolean cell as 1 / 0 / None; any other value fails."""
    if value is None:
        return None
    assert not isinstance(value, str), f"not a boolean: {value!r}"
    assert value in (True, False, 0, 1), f"not a boolean: {value!r}"
    return int(value)


async def check_boolean_casts(engine: SlayerQueryEngine, *, dataset: str) -> None:
    """``CAST`` / ``TRY_CAST(<id - 3> AS BOOLEAN)`` as a dimension, a filter and an aggregate input."""
    id_ref = f"{dataset}.id"
    for spelling in (f"CAST({id_ref} - 3 AS BOOLEAN)", f"TRY_CAST({id_ref} - 3 AS BOOLEAN)",
                     f"CAST(({id_ref} - 3) / 4.0 AS BOOLEAN)", f"({id_ref} - 3)::BOOLEAN"):
        resp = await engine.execute(SlayerQuery.model_validate({
            "population": dataset, "dimensions": [id_ref, {"field": spelling, "alias": "b"}],
        }))
        got = {int(r[id_ref]): boolean_cell(r["b"]) for r in resp.data}
        assert got == {k: int(k != 3) for k in got}, (spelling, resp.sql)
    resp = await engine.execute(SlayerQuery.model_validate({
        "population": dataset, "dimensions": [id_ref], "filter": [f"CAST({id_ref} - 3 AS BOOLEAN) = TRUE"],
    }))
    kept = {int(r[id_ref]) for r in resp.data}
    assert 3 not in kept, resp.sql
    assert len(kept) > 1, resp.sql
    resp = await engine.execute(SlayerQuery.model_validate({
        "population": dataset, "dimensions": [id_ref], "filter": [f"NOT CAST({id_ref} - 3 AS BOOLEAN)"],
    }))
    assert {int(r[id_ref]) for r in resp.data} == {3}, resp.sql
    resp = await engine.execute(SlayerQuery.model_validate({"population": dataset, "metrics": [
        {"metric": f"sum(CAST(CAST({id_ref} - 3 AS BOOLEAN) AS INT))", "alias": "s"},
        {"metric": f"count({dataset}.*) FILTER (WHERE CAST({id_ref} - 3 AS BOOLEAN))", "alias": "n"},
        {"metric": f"count({dataset}.*)", "alias": "all"},
    ]}))
    row = resp.data[0]
    assert int(row["s"]) == int(row["n"]) == int(row["all"]) - 1, resp.sql
