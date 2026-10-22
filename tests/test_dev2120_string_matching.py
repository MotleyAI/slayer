"""Collation, the one LIKE pattern language, SQLite LIKE case (spec: queries/string-matching)."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.sqlite_conn import transaction
from tests._dev2120_fixtures import (
    EXEC_BACKENDS,
    TIER1,
    emit_lib,
    lib_engine,
    lib_ids,
    lib_model,
    lib_query,
    lib_values,
)
from tests._engine_helpers import seeded_exec_engine


@pytest.fixture(params=EXEC_BACKENDS)
async def engine(request):
    async with lib_engine(request.param) as e:
        yield e


@pytest.fixture
async def sqlite_engine():
    async with lib_engine("sqlite") as e:
        yield e


def _mode_a_model() -> SlayerModel:
    base = lib_model()
    return base.model_copy(update={"columns": [
        *base.columns, Column(name="is_ada", sql="word LIKE 'ada%'", type=DataType.BOOLEAN),
    ]})


def _nocase_seed(db_path: str) -> None:
    with transaction(db_path) as con:
        con.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT COLLATE NOCASE)")
        con.executemany("INSERT INTO people VALUES (?, ?)", [(1, "ada"), (2, "Ada"), (3, "bob")])


def _people_model() -> SlayerModel:
    return SlayerModel(name="people", sql_table="people", data_source="test", columns=[
        Column(name="id", type=DataType.INT, primary_key=True), Column(name="name", type=DataType.TEXT),
    ])


class TestCaseSensitivity:
    async def test_sqlite_like_is_case_sensitive(self, sqlite_engine: SlayerQueryEngine) -> None:
        assert lib_ids(await sqlite_engine.execute(lib_query(filters=["word LIKE 'ada%'"]))) == {3}

    async def test_sqlite_like_ignores_a_nocase_column(self) -> None:
        async with seeded_exec_engine(dialect="sqlite", seed=_nocase_seed, models=[_people_model()]) as (engine, _):
            resp = await engine.execute(SlayerQuery.model_validate({
                "source_model": "people", "dimensions": ["id"], "filters": ["name LIKE 'ada%'"]}))
            assert {int(r["people.id"]) for r in resp.data} == {1}

    async def test_ilike_is_case_insensitive(self, engine: SlayerQueryEngine) -> None:
        assert lib_ids(await engine.execute(lib_query(filters=["word ILIKE 'ADA%'"]))) == {3, 4}

    async def test_mode_a_like_on_sqlite_unchanged(self) -> None:
        async with lib_engine("sqlite", models=[_mode_a_model()]) as engine:
            values = lib_values(await engine.execute(lib_query({"expression": "is_ada", "name": "v"},
                                                               filters=["id IN (3, 4)"])))
            assert {k: bool(v) for k, v in values.items()} == {3: True, 4: True}


class TestPatternLanguage:
    @pytest.mark.parametrize(("filt", "expected"), [
        (r"code LIKE 'a\_%'", {3}),
        ("code LIKE '[ab]%'", {6}),
        (r"word LIKE 'c:\x%'", {10}),
        ("code LIKE prefix || '%'", {1, 3, 4, 6, 7, 8, 10}),
        ("code LIKE 'a*?%'", {8}),
        ("notes LIKE 'paid%late'", {1, 4}),
    ])
    async def test_spec_patterns(self, engine: SlayerQueryEngine, filt: str, expected: set[int]) -> None:
        assert lib_ids(await engine.execute(lib_query(filters=[filt]))) == expected

    async def test_variable_pattern(self, engine: SlayerQueryEngine) -> None:
        resp = await engine.execute(lib_query(filters=["code LIKE '{p}'"], variables={"p": "a\\_%"}))
        assert lib_ids(resp) == {3}


class TestEmission:
    @pytest.mark.parametrize("dialect", ["postgres", "mysql", "duckdb", "snowflake", "trino", "tsql"])
    async def test_escape_clause(self, dialect: str) -> None:
        sql = await emit_lib(lib_query(filters=["code LIKE 'a%'"]), dialect=dialect)
        assert "ESCAPE" in sql.upper(), sql

    @pytest.mark.parametrize("dialect", ["clickhouse", "bigquery"])
    async def test_native_backslash_escaping(self, dialect: str) -> None:
        sql = await emit_lib(lib_query(filters=["code LIKE 'a%'"]), dialect=dialect)
        assert "ESCAPE" not in sql.upper(), sql

    async def test_sqlite_like_is_glob(self) -> None:
        sql = await emit_lib(lib_query(filters=["code LIKE 'a%'"]), dialect="sqlite")
        assert "GLOB" in sql.upper() and "SLAYER_LIKE_GLOB(" in sql.upper(), sql

    async def test_sqlite_ilike_is_native_like(self) -> None:
        sql = await emit_lib(lib_query(filters=["code ILIKE 'a%'"]), dialect="sqlite")
        assert " LIKE " in sql.upper() and "GLOB" not in sql.upper(), sql

    async def test_sqlite_mode_a_like_not_rewritten(self) -> None:
        sql = await emit_lib(lib_query({"expression": "is_ada", "name": "v"}), dialect="sqlite",
                             models=[_mode_a_model()])
        assert "GLOB" not in sql.upper(), sql

    async def test_sql_server_brackets_a_literal_bracket(self) -> None:
        sql = await emit_lib(lib_query(filters=["code LIKE '[ab]%'"]), dialect="tsql")
        assert "[[]" in sql, sql

    async def test_sql_server_brackets_a_column_pattern(self) -> None:
        sql = await emit_lib(lib_query(filters=["code LIKE prefix || '%'"]), dialect="tsql")
        assert "REPLACE(" in sql.upper() and "[[]" in sql, sql

    @pytest.mark.parametrize("dialect", TIER1)
    async def test_every_pattern_form_emits(self, dialect: str) -> None:
        await emit_lib(lib_query(filters=[r"code LIKE 'a\_%'", "code NOT LIKE '[ab]%'", r"word ILIKE 'c:\x%'",
                                          "code LIKE prefix || '%'"]), dialect=dialect)
