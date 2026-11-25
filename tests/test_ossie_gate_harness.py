"""Spec: compatibility/ossie-compliance › cell states, logical-type comparison and the canonical fixture form."""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

import pytest
import sqlglot

from slayer.core.errors import UnknownFunctionError
from slayer.engine.query_engine import SlayerResponse
from tests._dev2446_required_manifest import ROWS, Row, refused
from tests._ossie_gate import (
    PASS, TIER1, Approx, Bracket, Now, declaration_problems, gap, logical_problem, multiset_problem, passes,
)
from tests._ossie_gate_fixtures import (
    CUSTOMER, Placement, Table, parse_fixture_sql, placed_model, seed_statements, table_statements,
    wide_table_fixture,
)
from tests._ossie_gate_suite import check_layer1

NOON = datetime.datetime(2026, 11, 25, 12, 0)
BRACKET = Bracket(start=NOON, end=NOON + datetime.timedelta(seconds=2))


# --------------------------------------------------------------------------- #
# Logical-type comparison
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("got", "want", "engine"), [
    pytest.param(True, True, "duckdb", id="bool"),
    pytest.param(1, True, "sqlite", id="int-boolean engine"),
    pytest.param(0, False, "tsql", id="bit"),
    pytest.param(Decimal("12.350"), 12.35, "postgres", id="decimal scale"),
    pytest.param(12.35, Decimal("12.35"), "duckdb", id="double as written"),
    pytest.param(32.0, 32, "sqlite", id="integral double"),
    pytest.param(None, None, "duckdb", id="null"),
    pytest.param("", "", "duckdb", id="empty string"),
    pytest.param("2024-01-15", datetime.date(2024, 1, 15), "sqlite", id="date text"),
    pytest.param(datetime.datetime(2024, 1, 15), datetime.date(2024, 1, 15), "mysql", id="date at midnight"),
    pytest.param("2024-01-15 10:30:00", datetime.datetime(2024, 1, 15, 10, 30), "sqlite", id="timestamp text"),
    pytest.param(datetime.datetime(2024, 1, 15, 10, 30, tzinfo=datetime.UTC), datetime.datetime(2024, 1, 15, 10, 30),
                 "postgres", id="aware timestamp"),
    pytest.param(datetime.timedelta(hours=10, minutes=30), datetime.time(10, 30), "mysql", id="mysql time"),
    pytest.param(1.0000000001, Approx(value=1), "duckdb", id="approximate"),
])
def test_equal_by_logical_type(got: Any, want: Any, engine: str) -> None:
    assert logical_problem(got, want, engine=engine) is None


@pytest.mark.parametrize(("got", "want", "engine"), [
    pytest.param(1, True, "duckdb", id="TRUE vs 1 on a boolean engine"),
    pytest.param(True, 1, "sqlite", id="1 vs TRUE: an integer row"),
    pytest.param(12.350000000000001, 12.35, "duckdb", id="12.35 vs 12.350000000000001"),
    pytest.param(None, "", "duckdb", id="NULL vs empty"),
    pytest.param("", None, "duckdb", id="empty vs NULL"),
    pytest.param("3", 3, "duckdb", id="text vs number"),
    pytest.param(datetime.datetime(2024, 1, 15, 10, 15), datetime.date(2024, 1, 15), "duckdb", id="timestamp vs date"),
    pytest.param(datetime.datetime(2024, 1, 15, 10, 31), datetime.datetime(2024, 1, 15, 10, 30), "duckdb",
                 id="minute off"),
    pytest.param(datetime.datetime(2024, 1, 15, 10, 30, 0, 500), datetime.datetime(2024, 1, 15, 10, 30), "duckdb",
                 id="fractional second off"),
    pytest.param(1.1, Approx(value=1), "duckdb", id="outside tolerance"),
])
def test_unequal_by_logical_type(got: Any, want: Any, engine: str) -> None:
    assert logical_problem(got, want, engine=engine) is not None


def test_the_clock_bracket_is_widened_by_a_day() -> None:
    assert logical_problem(NOON.date(), Now(kind="date"), engine="trino", bracket=BRACKET) is None
    assert logical_problem(NOON.date() + datetime.timedelta(days=1), Now(kind="date"), engine="trino",
                           bracket=BRACKET) is None
    assert logical_problem(NOON.date() + datetime.timedelta(days=2), Now(kind="date"), engine="trino",
                           bracket=BRACKET) is not None
    assert logical_problem(NOON - datetime.timedelta(hours=5), Now(kind="timestamp"), engine="trino",
                           bracket=BRACKET) is None
    assert logical_problem(NOON - datetime.timedelta(days=3), Now(kind="timestamp"), engine="trino",
                           bracket=BRACKET) is not None
    assert logical_problem(NOON.date(), Now(kind="date"), engine="trino") is not None


def test_rows_compare_as_a_multiset() -> None:
    assert multiset_problem([("a", 1), ("b", 2), ("a", 1)], [("a", 1), ("a", 1), ("b", 2)], engine="duckdb") is None
    assert multiset_problem([("a", 1), ("b", 2)], [("a", 1), ("a", 1), ("b", 2)], engine="duckdb") is not None
    assert multiset_problem([("2023", "x")], [(Decimal(2023), "x")], engine="duckdb") is not None


# --------------------------------------------------------------------------- #
# Cell states
# --------------------------------------------------------------------------- #
def test_declarations_equal_the_discovered_cells() -> None:
    assert declaration_problems({"a": (PASS,), "b": (gap("DEV-0", ValueError, "sqlite"), passes(*TIER1[1:]))},
                                ["a", "b"]) == []


def test_a_stale_declaration_fails() -> None:
    assert declaration_problems({"a": (PASS,), "gone": (PASS,)}, ["a"]) == ["stale declaration: gone"]


def test_an_undeclared_case_fails() -> None:
    assert declaration_problems({}, ["new_case"]) == ["undeclared: new_case"]


def test_a_cell_is_declared_once() -> None:
    assert declaration_problems({"a": (PASS, passes("sqlite"))}, ["a"]) == ["a: sqlite declared twice"]
    assert declaration_problems({"a": (passes("sqlite"),)}, ["a"])[0] == "a: no state on duckdb"


class _Answering:
    """An engine that answers every query with ``v = 2``."""

    async def execute(self, query: Any, *args: Any, **kwargs: Any) -> SlayerResponse:
        return SlayerResponse(data=[{"v": 2}])


async def test_a_wrong_value_is_never_a_refusal() -> None:
    row = refused("Regular Expressions (RECOMMENDED)", "REGEXP_COUNT", "REGEXP_COUNT(orders.status, 'n')", 2,
                  error=UnknownFunctionError)
    with pytest.raises(pytest.fail.Exception):
        await check_layer1(_Answering(), row, engine_name="duckdb")


async def test_a_wrong_value_fails_a_passing_cell() -> None:
    row = Row(section="Basic Math (REQUIRED)", form="ABS", expression="ABS(-3)", value=3)
    with pytest.raises(AssertionError):
        await check_layer1(_Answering(), row, engine_name="duckdb")


def test_a_required_row_is_never_refused_everywhere() -> None:
    assert not [r.id for r in ROWS if r.required and all(r.state(e).state == "refused" for e in TIER1)]


# --------------------------------------------------------------------------- #
# Canonical fixture form
# --------------------------------------------------------------------------- #
def test_upstream_fixture_sql_reads_into_canonical_form() -> None:
    tables = {(t.schema_name, t.name): t for t in wide_table_fixture("stores")}
    sales = tables[("stores", "store_sales")]
    assert sales.columns[-1] == ("sales_amount", "INT")
    assert ("SS4", "I1", "D2023", "S1", None, 25) in sales.rows
    assert tables[("stores", "customers")].rows[-1] == ("C3", None)
    assert {t.name for t in wide_table_fixture("retail")} == {"customers", "orders", "returns", "visits", "tickets"}


@pytest.mark.parametrize("sql", [
    "UPDATE s.t SET a = 1",
    "CREATE TABLE s.t (a GEOMETRY)",
    "CREATE TABLE s.t (a INTEGER CHECK (a > 0))",
    "CREATE TABLE s.t (a INTEGER); INSERT INTO s.t (a) VALUES (1)",
    "CREATE TABLE s.t (a INTEGER); INSERT INTO s.t SELECT 1",
    "INSERT INTO s.u VALUES (1)",
])
def test_an_unclassified_fixture_statement_fails_closed(sql: str) -> None:
    with pytest.raises(ValueError, match="unclassified|does not match"):
        parse_fixture_sql(sql)


def test_a_typed_fixture_value_reads() -> None:
    (table,) = parse_fixture_sql("CREATE TABLE t (d DATE, n DOUBLE); INSERT INTO t VALUES ('2024-01-15', -2.5)")
    assert table.rows == ((datetime.date(2024, 1, 15), -2.5),)


@pytest.mark.parametrize("engine", TIER1)
def test_every_engine_emits_parseable_statements(engine: str) -> None:
    for statement in seed_statements(Placement(engine=engine, token="abc")):
        assert sqlglot.parse_one(statement, read=engine) is not None, statement


@pytest.mark.parametrize(("engine", "quoted"), [
    ("postgres", '"externalId"'), ("snowflake", '"externalId"'), ("mysql", "`externalId`"),
    ("tsql", "[externalId]"), ("bigquery", "`externalId`"), ("clickhouse", '"externalId"'),
])
def test_a_mixed_case_column_is_created_quoted(engine: str, quoted: str) -> None:
    assert quoted in table_statements(Placement(engine=engine), CUSTOMER)[0]


def test_a_plain_name_is_created_bare_so_every_engine_folds_it_alike() -> None:
    table = Table(name="orders", columns=(("id", "INT"),))
    assert table_statements(Placement(engine="snowflake"), table)[0] == "CREATE TABLE orders (id INTEGER)"


@pytest.mark.parametrize(("engine", "token", "source"), [
    ("duckdb", "", "retail.customers"), ("sqlite", "", "retail__customers"),
    ("snowflake", "x1", "retail_x1.customers"), ("bigquery", "x1", "retail_x1.customers"),
])
def test_a_model_source_names_the_engines_table(engine: str, token: str, source: str) -> None:
    text = "datasets:\n  - name: customers\n    source: retail.customers\n"
    assert placed_model(text, Placement(engine=engine, token=token)) == text.replace("retail.customers", source)
