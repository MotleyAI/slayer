"""The gate's checks, shared by the in-process and the server test modules, and each wide-table case's state."""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from collections.abc import Mapping
from typing import Any, Protocol

import pytest
from pydantic import BaseModel

from slayer.core.errors import OssieQueryError, SlayerError
from slayer.engine.query_engine import SlayerResponse
from tests._dev2215_fixtures import q
from tests._dev2217_fixtures import STRICT, WIDE_TABLE
from tests._dev2446_required_manifest import Row
from tests._ossie_gate import PASS, Bracket, Declared, assert_logical, expand, multiset_problem, utc_now
from tests._ossie_gate_fixtures import Provider, Table, gate_tables, select_statement


class QueryRunner(Protocol):
    async def execute(self, query: Any, *args: Any, **kwargs: Any) -> SlayerResponse: ...


# --------------------------------------------------------------------------- #
# Seeding
# --------------------------------------------------------------------------- #
def seeded_tables() -> list[Table]:
    return [t for group in gate_tables().values() for t in group]


def check_seeded(provider: Provider, table: Table, *, engine: str) -> None:
    """``table`` reads back with its canonical rows."""
    got = provider.fetch(select_statement(provider.placement, table))
    problem = multiset_problem(got, table.rows, engine=engine)
    assert problem is None, problem


# --------------------------------------------------------------------------- #
# Layer 1
# --------------------------------------------------------------------------- #
NULL_PLACEMENT = "DEFAULT NULL PLACEMENT"
#: ``RANK() OVER (ORDER BY orders.amount DESC)`` per order id: NULL sorts first for DESC (row 5's amount is NULL).
NULL_PLACEMENT_RANKS = {5: 1, 6: 2, 4: 3, 3: 4, 2: 5, 1: 6}


async def layer1_value(engine: QueryRunner, row: Row) -> tuple[Any, Bracket]:
    """``row`` evaluated: an aggregate over every order, a scalar or window at order 3."""
    if row.kind == "aggregate":
        query = q(metrics=[{"metric": row.expression, "alias": "v"}])
    else:
        query = q(dimensions=[{"field": row.expression, "alias": "v"}], filter="orders.id = 3")
    start = utc_now()
    response = await engine.execute(query)
    end = utc_now()
    assert len(response.data) == 1, response.data
    return response.data[0]["v"], Bracket(start=start, end=end)


async def check_layer1(engine: QueryRunner, row: Row, *, engine_name: str) -> None:
    """Ossie's value, or the declared refusal; a gap runs as a strict expected failure."""
    declared = row.state(engine_name)
    if row.kind == "reserved":
        with pytest.raises(SlayerError):
            await layer1_value(engine, row)
        return
    if declared.state == "refused":
        assert declared.error is not None
        with pytest.raises(declared.error):
            await layer1_value(engine, row)
        return
    if row.form == NULL_PLACEMENT:  # one row ranks 1 under any NULL order: rank every order
        response = await engine.execute(q(dimensions=[{"field": "orders.id", "alias": "id"},
                                                      {"field": row.expression, "alias": "v"}]))
        problem = multiset_problem([(r["id"], r["v"]) for r in response.data], list(NULL_PLACEMENT_RANKS.items()),
                                   engine=engine_name)
        assert problem is None, problem
        return
    await check_layer1_value(engine, row, engine_name=engine_name)


async def check_layer1_value(engine: QueryRunner, row: Row, *, engine_name: str) -> None:
    """Ossie's value, whatever the cell's declared state."""
    value, bracket = await layer1_value(engine, row)
    assert_logical(value, row.value, engine=engine_name, bracket=bracket)


# --------------------------------------------------------------------------- #
# Wide-table cases
# --------------------------------------------------------------------------- #
Cell = Decimal | str | None


class Expected(BaseModel):
    fixture: str
    header: list[str] = []
    rows: list[list[Cell]] = []
    error: str | None = None


def _cell(text: str) -> Cell:
    """A value as written: empty is NULL, ``""`` the empty string, a number its exact value."""
    if text == "":
        return None
    if text == '""':
        return ""
    try:
        return Decimal(text)
    except InvalidOperation:
        return text


def read_expected(case: Path) -> Expected:
    lines = (case / "expected.txt").read_text(encoding="utf-8").splitlines()
    fixture = next(line.split(":", 1)[1].strip() for line in lines if line.startswith("# fixture:"))
    error = next((line.removeprefix("> error:").strip() for line in lines if line.startswith("> error:")), None)
    if error is not None:
        return Expected(fixture=fixture, error=error)
    table = [[c.strip() for c in line.strip().strip("|").split("|")] for line in lines if line.startswith("|")]
    return Expected(fixture=fixture, header=table[0], rows=[[_cell(c) for c in row] for row in table[1:]])


def case_query(case: Path) -> dict[str, Any]:
    return json.loads((case / "query.json").read_text(encoding="utf-8"))


def translate(query: dict[str, Any]) -> dict[str, Any]:
    """Ossie's JSON binding (wide_table §3.7) as a root-less SLayer query under the default conformance."""
    out = {key: value for key, value in query.items() if key != "filter"}
    if "filter" in query:
        out["filters"] = [query["filter"]]
    return out


def result_keys(query: dict[str, Any]) -> list[str]:
    """Each entry's native result key, in Ossie's output order: dimensions, then metrics."""
    keys = []
    for kind, entries in (("field", query.get("dimensions") or []), ("metric", query.get("metrics") or [])):
        keys += [entry if isinstance(entry, str) else entry.get("alias") or entry[kind] for entry in entries]
    return keys


def cases() -> list[Path]:
    return sorted(p for p in (WIDE_TABLE / "cases").iterdir() if p.is_dir())


def row_cases() -> list[Path]:
    return [c for c in cases() if c.name.startswith("a")]


def rows_problem(response: SlayerResponse, keys: list[str], expected: Expected, *, engine: str) -> str | None:
    got = [tuple(row[key] for key in keys) for row in response.data]
    return multiset_problem(got, expected.rows, engine=engine)


async def check_case_under_strict(engines: Mapping[str, QueryRunner], case: Path, *, engine_name: str) -> None:
    """Ossie's rows, header and order, or Ossie's error code."""
    expected = read_expected(case)
    engine = engines[expected.fixture]
    if expected.error is not None:
        with pytest.raises(OssieQueryError) as raised:
            await engine.execute(case_query(case), conformance=STRICT)
        assert raised.value.code == expected.error, raised.value
        return
    response = await engine.execute(case_query(case), conformance=STRICT)
    assert response.columns == expected.header
    problem = rows_problem(response, expected.header, expected, engine=engine_name)
    assert problem is None, f"{problem}\n{response.sql}"


async def check_row_case_under_the_default(engines: Mapping[str, QueryRunner], case: Path, *,
                                           engine_name: str) -> None:
    expected = read_expected(case)
    query = case_query(case)
    response = await engines[expected.fixture].execute(translate(query))
    keys = result_keys(query)
    assert len(keys) == len(expected.header)
    problem = rows_problem(response, keys, expected, engine=engine_name)
    assert problem is None, f"{problem}\n{response.sql}"


#: Every upstream wide-table case's state on every Tier-1 engine.
CASE_STATES: dict[str, tuple[Declared, ...]] = {name: (PASS,) for name in (
    "a01_shared_dimension", "a02_count_empty_input", "a03_three_branches", "a04_filter_on_dimension",
    "a05_groups_from_orders", "a06_groups_from_returns", "a07_dimension_only", "a08_model_metric_components",
    "a09_aliases", "a10_filter_without_dimension", "a11_filter_on_home_dataset", "a12_anchor_from_dimensions",
    "a13_filter_false_with_dimension", "a14_filter_false_without_dimension", "a15_unconnected_filter_false",
    "a16_shared_dimensions_stores", "a17_unmatched_home_null_group", "a18_anchor_with_metric", "a19_sales_by_year",
    "a20_filter_on_sale_year", "a21_single_path_on_ambiguous_model",
    "e01_invalid_query_unknown_key", "e02_invalid_query_null", "e03_empty_aggregation_query", "e04_name_not_found",
    "e05_metric_in_dimensions", "e06_field_in_metrics", "e07_aggregate_in_filter", "e08_window_in_filter",
    "e09_cross_dataset_filter", "e10_name_collision", "e11_unaggregated_field_in_metric", "e12_windowed_metric",
    "e13_measure_grain_two_datasets", "e14_measure_grain_no_dataset", "e15_no_path",
    "e16_partial_dimension_reachability", "e17_no_anchor", "e18_filter_path_from_metric",
    "e19_filter_path_from_anchor", "e20_order_invalid_before_name", "e21_order_name_before_filter",
    "e22_no_path_stores", "e23_no_anchor_stores", "e24_ambiguous_path_dimension", "e25_ambiguous_path_filter",
)}


def case_state(case: str, engine: str) -> Declared:
    """An undeclared case runs as pass; the declaration check reports it."""
    return expand(case, CASE_STATES.get(case, (PASS,)))[engine]


# --------------------------------------------------------------------------- #
# A mixed-case source column read through a relationship
# --------------------------------------------------------------------------- #
MIXED_CASE_QUERY = {
    "population": "invoice", "dimensions": ["customer.externalId"],
    "metrics": [{"metric": "count(customer.externalId)", "alias": "n"},
                {"metric": "sum(invoice.totalAmount)", "alias": "total"}],
    "filter": "customer.externalId <> 'EXT-9'",
}
#: ``count`` runs over the customer rows, ``sum`` over the invoices.
MIXED_CASE_ROWS = [("EXT-1", 1, 15), ("EXT-2", 1, 7)]
ROOTED_QUERY = {"population": "customer", "dimensions": ["customer.externalId"],
                "metrics": [{"metric": "count(customer.externalId)", "alias": "n"}],
                "filter": "customer.externalId <> 'EXT-9'"}
ROOTED_ROWS = [("EXT-1", 1), ("EXT-2", 1)]


async def check_rows(engine: QueryRunner, query: dict[str, Any], want: list[tuple[Any, ...]], *,
                     engine_name: str) -> None:
    response = await engine.execute(query)
    got = [tuple(row[c] for c in response.columns) for row in response.data]
    problem = multiset_problem(got, want, engine=engine_name)
    assert problem is None, f"{problem}\n{response.sql}"
