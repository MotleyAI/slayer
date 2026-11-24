"""A parenthesised single literal after ``in`` / ``not in`` (spec: queries/time-points › Single-string in tests
period membership)."""

from __future__ import annotations

import sqlite3
from collections.abc import AsyncGenerator
from typing import Any, Optional

import duckdb
import pytest

from slayer.core.errors import DateOperandTypeError
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.syntax import parse_filter_expr
from tests._engine_helpers import seeded_exec_engine


def _seed_tables(db_path: str, tables: dict[str, tuple[str, list[tuple[Any, ...]]]]) -> None:
    """Create and fill ``{name: (column DDL, rows)}`` in the SQLite or DuckDB file at ``db_path``."""
    con = duckdb.connect(db_path) if db_path.endswith(".duckdb") else sqlite3.connect(db_path)
    try:
        for name, (columns, rows) in tables.items():
            con.execute(f"CREATE TABLE {name} ({columns})")
            con.executemany(f"INSERT INTO {name} VALUES ({', '.join('?' * len(rows[0]))})", rows)
        con.commit()
    finally:
        con.close()


def _seed(db_path: str) -> None:
    _seed_tables(db_path, {"invoices": ("id INTEGER, state VARCHAR, amount DOUBLE, issued_at TIMESTAMP", [
        (1, "paid", 10.0, "2025-03-01 00:00:00"), (2, "sent", 20.0, "2025-06-01 00:00:00"),
        (3, "paid", 30.0, "2024-12-01 00:00:00"), (5, "draft", 50.0, "2025-01-15 00:00:00"),
    ])})


def _models() -> list[SlayerModel]:
    return [SlayerModel.model_validate({
        "name": "invoices", "sql_table": "invoices", "data_source": "test",
        "columns": [{"name": "id", "type": "number", "primary_key": True},
                    {"name": "state", "type": "string"},
                    {"name": "amount", "type": "number"},
                    {"name": "issued_at", "type": "time"}],
    })]


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect=request.param, seed=_seed, models=_models()) as (e, _):
        yield e


def _query(formula: str = "sum(amount)", *, filters: Optional[list[str]] = None) -> SlayerQuery:
    body: dict[str, Any] = {"source_model": "invoices", "measures": [{"formula": formula, "name": "v"}]}
    if filters is not None:
        body["filters"] = filters
    return SlayerQuery.model_validate(body)


async def _value(engine: SlayerQueryEngine, query: SlayerQuery) -> float:
    resp = await engine.execute(query)
    [row] = resp.data
    return float(row["invoices.v"])


async def _sql(engine: SlayerQueryEngine, query: SlayerQuery) -> str:
    sql = (await engine.execute(query, dry_run=True)).sql
    assert sql is not None
    return sql


class TestFilters:
    @pytest.mark.parametrize(("predicate", "expected"), [
        ("state in ('paid')", 40.0),
        ("state not in ('paid')", 70.0),
        ("id in (5)", 50.0),
        ("id not in (5)", 60.0),
    ])
    async def test_value(self, engine: SlayerQueryEngine, predicate: str, expected: float) -> None:
        assert await _value(engine, _query(filters=[predicate])) == expected

    @pytest.mark.parametrize(("predicate", "tuple_form"), [
        ("state in ('paid')", "state in ('paid',)"),
        ("state not in ('paid')", "state not in ('paid',)"),
        ("id in (5)", "id in (5,)"),
    ])
    async def test_same_sql_as_the_one_element_tuple(
        self, engine: SlayerQueryEngine, predicate: str, tuple_form: str,
    ) -> None:
        assert await _sql(engine, _query(filters=[predicate])) == await _sql(engine, _query(filters=[tuple_form]))


class TestMeasureConditions:
    @pytest.mark.parametrize(("formula", "expected"), [
        ("sum(iif(state in ('paid'), amount, 0))", 40.0),
        ("sum(iif(state not in ('paid'), amount, 0))", 70.0),
        ("sum(iif(id in (5), amount, 0))", 50.0),
    ])
    async def test_value(self, engine: SlayerQueryEngine, formula: str, expected: float) -> None:
        assert await _value(engine, _query(formula)) == expected

    @pytest.mark.parametrize(("formula", "tuple_form"), [
        ("sum(iif(state in ('paid'), amount, 0))", "sum(iif(state in ('paid',), amount, 0))"),
        ("sum(iif(id in (5), amount, 0))", "sum(iif(id in (5,), amount, 0))"),
    ])
    async def test_same_sql_as_the_one_element_tuple(
        self, engine: SlayerQueryEngine, formula: str, tuple_form: str,
    ) -> None:
        assert await _sql(engine, _query(formula)) == await _sql(engine, _query(tuple_form))


class TestParser:
    def test_parenthesised_number_is_the_one_element_tuple(self) -> None:
        assert parse_filter_expr("id in (5)") == parse_filter_expr("id in (5,)")

    def test_parenthesised_string_is_not_the_bare_string(self) -> None:
        assert parse_filter_expr("state in ('paid')") != parse_filter_expr("state in 'paid'")


async def test_bare_string_against_text_stays_refused(engine: SlayerQueryEngine) -> None:
    with pytest.raises(DateOperandTypeError, match="state"):
        await engine.execute(_query(filters=["state in 'paid'"]))


class TestParenthesisedStringAgainstTimestampStaysAPeriod:
    """1.x only: main reads ``ts in ('2025')`` as a value list."""

    async def test_filter(self, engine: SlayerQueryEngine) -> None:
        parenthesised = _query(filters=["issued_at in ('2025')"])
        assert await _value(engine, parenthesised) == 80.0
        assert await _sql(engine, parenthesised) == await _sql(engine, _query(filters=["issued_at in '2025'"]))

    async def test_measure_condition(self, engine: SlayerQueryEngine) -> None:
        assert await _value(engine, _query("sum(iif(issued_at in ('2025'), amount, 0))")) == 80.0
