"""Ossie wide-table Appendix B (B.1–B.5) under the strict binding: exact rows and columns, or the specified error.

Spec: queries/ossie-l3-binding › Admitted queries return Ossie's rows; Paths decide reachability and ambiguity.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests._dev2217_fixtures import fixture_engine, rows, strict, strict_error

REGION = "customers.region"
REVENUE = "orders.total_revenue"
RETURNS = "returns.total_returns"

#: (fixture, query, columns, rows), rows sorted as ``rows()`` sorts them.
ROWS: list[Any] = [
    pytest.param("retail", {"dimensions": [REGION], "metrics": [REVENUE, RETURNS]},
                 ["region", "total_revenue", "total_returns"],
                 [("EAST", 350.0, 25.0), ("NORTH", None, 10.0), ("WEST", 300.0, None)], id="B.1-shared-dimension"),
    pytest.param("retail", {"dimensions": [REGION], "metrics": [REVENUE, "returns.return_count"]},
                 ["region", "total_revenue", "return_count"],
                 [("EAST", 350.0, 1.0), ("NORTH", None, 1.0), ("WEST", 300.0, 0.0)], id="B.1-count-empty-input"),
    pytest.param("retail", {"dimensions": [REGION], "metrics": [REVENUE, RETURNS],
                            "filter": "customers.region <> 'EAST'"},
                 ["region", "total_revenue", "total_returns"],
                 [("NORTH", None, 10.0), ("WEST", 300.0, None)], id="B.1-filter-on-dimension"),
    pytest.param("retail", {"dimensions": [REGION], "metrics": [REVENUE, RETURNS, "visits.visit_count"]},
                 ["region", "total_revenue", "total_returns", "visit_count"],
                 [("EAST", 350.0, 25.0, 0.0), ("NORTH", None, 10.0, 2.0), ("WEST", 300.0, None, 1.0)],
                 id="B.2-three-branches"),
    pytest.param("stores", {"dimensions": ["item.brand", "store.state"], "metrics": ["store_sales.total_sales"]},
                 ["brand", "state", "total_sales"], [("Acme", "CA", 325.0), ("Beta", "NY", 90.0)],
                 id="B.3-metric-makes-an-anchor"),
    pytest.param("retail", {"metrics": [REVENUE, "tickets.ticket_count"]}, ["total_revenue", "ticket_count"],
                 [(650.0, 0.0)], id="B.4-unconnected-unfiltered"),
    pytest.param("retail", {"metrics": [REVENUE, "tickets.ticket_count"], "filter": "FALSE"},
                 ["total_revenue", "ticket_count"], [(None, 0.0)], id="B.4-unconnected-filter-false"),
    pytest.param("retail", {"metrics": [REVENUE], "filter": "customers.region = 'EAST'"}, ["total_revenue"],
                 [(350.0,)], id="B.5-filter-without-dimension"),
    pytest.param("retail", {"dimensions": [REGION], "metrics": [REVENUE], "filter": "orders.amount > 100"},
                 ["region", "total_revenue"], [("EAST", 200.0), ("WEST", 300.0)], id="B.5-filter-on-home"),
    pytest.param("retail", {"dimensions": ["orders.order_id", REGION], "filter": "orders.amount > 100"},
                 ["order_id", "region"], [("O2", "EAST"), ("O4", "WEST")], id="B.5-anchor-from-dimensions"),
    pytest.param("retail", {"dimensions": [REGION], "metrics": [REVENUE], "filter": "FALSE"},
                 ["region", "total_revenue"], [], id="B.5-filter-false-with-dimension"),
    pytest.param("retail", {"metrics": [REVENUE], "filter": "FALSE"}, ["total_revenue"], [(None,)],
                 id="B.5-filter-false-without-dimension"),
]

#: (fixture, query, code, attribute → value).
ERRORS: list[Any] = [
    pytest.param("stores_ambiguous", {"dimensions": ["date_dim.year"], "metrics": ["store_sales.total_sales"]},
                 "E_AMBIGUOUS_PATH", {}, id="B.3-ambiguous-dimension-path"),
    pytest.param("stores_ambiguous", {"metrics": ["store_sales.total_sales"], "filter": "date_dim.year = 2024"},
                 "E_AMBIGUOUS_PATH", {}, id="B.3-ambiguous-filter-path"),
    pytest.param("stores", {"dimensions": ["item.brand"], "metrics": ["customers.customer_count"]}, "E_NO_PATH",
                 {"dimension": "item.brand", "metric": "customers.customer_count"}, id="B.3-no-path"),
    pytest.param("stores", {"dimensions": ["item.brand", "store.state"]}, "E_NO_ANCHOR", {}, id="B.3-no-anchor"),
    pytest.param("retail", {"dimensions": [REGION], "metrics": [REVENUE, "tickets.ticket_count"]},
                 "E_PARTIAL_DIMENSION_REACHABILITY", {"dimension": REGION, "metric": "tickets.ticket_count"},
                 id="B.4-partial-reachability"),
    pytest.param("retail", {"dimensions": [REGION], "metrics": [REVENUE, RETURNS], "filter": "orders.amount > 100"},
                 "E_UNSUPPORTED_FILTER_PATH", {"metric": RETURNS}, id="B.5-filter-path-from-metric"),
    pytest.param("retail", {"dimensions": [REGION], "filter": "orders.amount > 100"}, "E_UNSUPPORTED_FILTER_PATH",
                 {"anchor": "customers"}, id="B.5-filter-path-from-anchor"),
]


@pytest.mark.parametrize(("fixture", "query", "columns", "expected"), ROWS)
async def test_appendix_b_rows(tmp_path: Path, fixture: str, query: dict[str, Any], columns: list[str],
                               expected: list[tuple[Any, ...]]) -> None:
    async with fixture_engine(tmp_path, fixture) as engine:
        response = await strict(engine, query)
    assert response.columns == columns
    assert rows(response) == expected


@pytest.mark.parametrize(("fixture", "query", "code", "attributes"), ERRORS)
async def test_appendix_b_errors(tmp_path: Path, fixture: str, query: dict[str, Any], code: str,
                                 attributes: dict[str, str]) -> None:
    async with fixture_engine(tmp_path, fixture) as engine:
        exc = await strict_error(engine, query, code)
    assert {name: getattr(exc, name) for name in attributes} == attributes


@pytest.mark.xfail(strict=True, reason="DEV-2388: a metric binds through its host, not from its own home")
async def test_appendix_b3_shared_dimensions(tmp_path: Path) -> None:
    async with fixture_engine(tmp_path, "stores") as engine:
        response = await strict(engine, {"dimensions": ["item.brand", "date_dim.year"],
                                         "metrics": ["store_sales.total_sales", "store_returns.total_returns"]})
    assert response.columns == ["brand", "year", "total_sales", "total_returns"]
    assert rows(response) == [("Acme", 2023.0, 125.0, 10.0), ("Acme", 2024.0, 200.0, None),
                              ("Beta", 2024.0, 90.0, 5.0)]
