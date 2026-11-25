"""A parenthesised single literal after ``in`` / ``not in`` (spec: queries/time-points › Single-string in tests
period membership)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, Optional

import pytest

from slayer.core.errors import DateOperandTypeError
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.syntax import parse_expr
from slayer.engine.query_engine import SlayerQueryEngine
from tests._engine_helpers import seed_tables, seeded_exec_engine


def _seed(db_path: str) -> None:
    seed_tables(db_path, {"invoices": ("id INTEGER, state VARCHAR, amount DOUBLE, issued_at TIMESTAMP", [
        (1, "paid", 10.0, "2025-03-01 00:00:00"), (2, "sent", 20.0, "2025-06-01 00:00:00"),
        (3, "paid", 30.0, "2024-12-01 00:00:00"), (5, "draft", 50.0, "2025-01-15 00:00:00"),
    ])})


def _models() -> list[SlayerDataset]:
    return [SlayerDataset.model_validate({
        "name": "invoices", "sql_table": "invoices", "data_source": "test",
        "fields": [{"name": "id", "datatype": "Integer", "primary_key": True},
                   {"name": "state", "datatype": "String"},
                   {"name": "amount", "datatype": "Float"},
                   {"name": "issued_at", "datatype": "DateTime"}],
    })]


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect=request.param, seed=_seed, datasets=_models()) as (e, _):
        yield e


def _query(formula: str = "sum(invoices.amount)", *, filters: Optional[list[str]] = None) -> SlayerQuery:
    body: dict[str, Any] = {"population": "invoices", "metrics": [{"metric": formula, "alias": "v"}]}
    if filters is not None:
        body["filter"] = " AND ".join(filters)
    return SlayerQuery.model_validate(body)


async def _value(engine: SlayerQueryEngine, query: SlayerQuery) -> float:
    resp = await engine.execute(query)
    [row] = resp.data
    return float(row["v"])


async def _sql(engine: SlayerQueryEngine, query: SlayerQuery) -> str:
    sql = (await engine.execute(query, dry_run=True)).sql
    assert sql is not None
    return sql


class TestFilters:
    @pytest.mark.parametrize(("predicate", "expected"), [
        ("invoices.state in ('paid')", 40.0),
        ("invoices.state not in ('paid')", 70.0),
        ("invoices.id in (5)", 50.0),
        ("invoices.id not in (5)", 60.0),
    ])
    async def test_value(self, engine: SlayerQueryEngine, predicate: str, expected: float) -> None:
        assert await _value(engine, _query(filters=[predicate])) == expected

    @pytest.mark.parametrize(("predicate", "tuple_form"), [
        ("invoices.state in ('paid')", "invoices.state in ('paid',)"),
        ("invoices.state not in ('paid')", "invoices.state not in ('paid',)"),
        ("invoices.id in (5)", "invoices.id in (5,)"),
    ])
    async def test_same_sql_as_the_one_element_tuple(
        self, engine: SlayerQueryEngine, predicate: str, tuple_form: str,
    ) -> None:
        assert await _sql(engine, _query(filters=[predicate])) == await _sql(engine, _query(filters=[tuple_form]))


class TestMeasureConditions:
    @pytest.mark.parametrize(("formula", "expected"), [
        ("sum(iif(invoices.state in ('paid'), invoices.amount, 0))", 40.0),
        ("sum(iif(invoices.state not in ('paid'), invoices.amount, 0))", 70.0),
        ("sum(iif(invoices.id in (5), invoices.amount, 0))", 50.0),
    ])
    async def test_value(self, engine: SlayerQueryEngine, formula: str, expected: float) -> None:
        assert await _value(engine, _query(formula)) == expected

    @pytest.mark.parametrize(("formula", "tuple_form"), [
        ("sum(iif(invoices.state in ('paid'), invoices.amount, 0))",
         "sum(iif(invoices.state in ('paid',), invoices.amount, 0))"),
        ("sum(iif(invoices.id in (5), invoices.amount, 0))", "sum(iif(invoices.id in (5,), invoices.amount, 0))"),
    ])
    async def test_same_sql_as_the_one_element_tuple(
        self, engine: SlayerQueryEngine, formula: str, tuple_form: str,
    ) -> None:
        assert await _sql(engine, _query(formula)) == await _sql(engine, _query(tuple_form))


class TestParser:
    def test_parenthesised_number_is_the_one_element_tuple(self) -> None:
        assert parse_expr("id in (5)") == parse_expr("id in (5,)")

    @pytest.mark.parametrize("text", [
        "active in (true)", "active not in (FALSE)", "active in (True)", "state in (NULL)", "state not in (None)",
    ])
    def test_parenthesised_boolean_in_any_case_is_the_one_element_tuple(self, text: str) -> None:
        assert parse_expr(text) == parse_expr(text.replace(")", ",)"))

    @pytest.mark.parametrize(("redundant", "single"), [
        ("state in (('paid'))", "state in ('paid')"),
        ("id in ((5))", "id in (5)"),
        ("id not in (((-5)))", "id not in (-5)"),
    ])
    def test_redundant_parentheses_read_as_one_pair(self, redundant: str, single: str) -> None:
        assert parse_expr(redundant) == parse_expr(single)

    def test_parenthesised_string_is_not_the_bare_string(self) -> None:
        assert parse_expr("state in ('paid')") != parse_expr("state in 'paid'")


async def test_bare_string_against_text_stays_refused(engine: SlayerQueryEngine) -> None:
    query = _query(filters=["invoices.state in 'paid'"])
    with pytest.raises(DateOperandTypeError, match="state"):
        await engine.execute(query)


class TestParenthesisedStringAgainstTimestampIsAValueList:
    """1.x keeps the period reading here; main reads ``ts in ('2025')`` as the one-element value list."""

    async def test_filter(self, engine: SlayerQueryEngine) -> None:
        parenthesised = await _sql(engine, _query(filters=["invoices.issued_at in ('2025')"]))
        assert parenthesised == await _sql(engine, _query(filters=["invoices.issued_at in ('2025',)"]))
        assert parenthesised != await _sql(engine, _query(filters=["invoices.issued_at in '2025'"]))

    async def test_measure_condition(self, engine: SlayerQueryEngine) -> None:
        parenthesised = await _sql(engine, _query("sum(iif(invoices.issued_at in ('2025'), invoices.amount, 0))"))
        tuple_form = await _sql(engine, _query("sum(iif(invoices.issued_at in ('2025',), invoices.amount, 0))"))
        assert parenthesised == tuple_form
