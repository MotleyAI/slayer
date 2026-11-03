"""Shared fixtures for the cast type grammar, array casts and the TRY_CAST integer rule (spec: queries/casts).

Underscore-prefixed so pytest skips collection.

tags (id, codes): 1 [1, 3] | 2 [2] | 3 NULL — ``codes`` is TEXT in each database's own array-text form.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._engine_helpers import seeded_exec_engine

#: Each database's text spelling of an INT array.
ARRAY_TEXT = {"postgres": ("{1,3}", "{2}"), "duckdb": ("[1, 3]", "[2]")}
CONTAINS_3 = frozenset({1})
LENGTHS = {1: 2, 2: 1, 3: None}


def tags_model(*, data_source: str = "test") -> SlayerModel:
    return SlayerModel(
        name="tags", sql_table="tags", data_source=data_source,
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="codes", type=DataType.TEXT)],
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
    async with seeded_exec_engine(dialect="duckdb", seed=_seed_duckdb_tags, models=[tags_model()]) as (engine, _):
        yield engine


def tags_query(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": "tags", **kw})


async def check_array_cast(engine: SlayerQueryEngine) -> None:
    """``contains`` / ``length`` over ``CAST(codes AS INT[])`` read it as a list of INT."""
    resp = await engine.execute(tags_query(dimensions=["id"], filters=["contains(CAST(codes AS INT[]), 3)"]))
    assert {int(r["tags.id"]) for r in resp.data} == CONTAINS_3, resp.sql
    resp = await engine.execute(tags_query(dimensions=["id", {"expression": "length(codes::INT[])", "name": "n"}]))
    got = {int(r["tags.id"]): (None if r["tags.n"] is None else int(r["tags.n"])) for r in resp.data}
    assert got == LENGTHS, resp.sql


# --------------------------------------------------------------------------- #
# TRY_CAST to an integer type: only integer text, else NULL, on every Tier-1 dialect
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
    ("CAST(1.5 AS DOUBLE)", "INT", None),
    ("CAST(2 AS DOUBLE)", "INT", 2),
    ("CAST(-3 AS DOUBLE)", "INT", -3),
    ("1.5", "INT", None),
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
]
TRY_CAST_CASE_IDS = [f"{src}->{to}" for src, to, _ in TRY_CAST_TARGET_CASES]


def try_cast_measures() -> list[dict[str, str]]:
    return [{"formula": f"max(TRY_CAST({src} AS {to}))", "name": f"c{i}"}
            for i, (src, to, _) in enumerate(TRY_CAST_TARGET_CASES)]


def _normalised(value: Any, *, target: str) -> Any:
    if value is None:
        return None
    return float(value) if target == "DOUBLE" else int(value)


async def check_try_cast_targets(engine: SlayerQueryEngine, *, model: str) -> None:
    """Every ``TRY_CAST_TARGET_CASES`` row in one query over any non-empty ``model``."""
    resp = await engine.execute(SlayerQuery.model_validate({"source_model": model, "measures": try_cast_measures()}))
    assert len(resp.data) == 1, resp.data
    row = resp.data[0]
    got = {TRY_CAST_CASE_IDS[i]: _normalised(row[f"{model}.c{i}"], target=to)
           for i, (_, to, _) in enumerate(TRY_CAST_TARGET_CASES)}
    want = {TRY_CAST_CASE_IDS[i]: expected for i, (_, _, expected) in enumerate(TRY_CAST_TARGET_CASES)}
    assert got == want, resp.sql
