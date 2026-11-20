"""The population and the row sources are reported on every surface (spec: queries/population › Population is
reported)."""

from __future__ import annotations

import json
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.query import SlayerQuery
from slayer.engine.cache import CacheConfig
from slayer.mcp.server import create_mcp_server
from slayer.memories.resolver import extract_entities_from_query
from slayer.storage.sqlite_conn import transaction

from tests._dev2212_fixtures import BACKENDS, q, run, seed_engine, seeded

R, REV, RET = "customers.region", "orders.total_revenue", "returns.total_returns"
TWO = {"dimensions": [R], "metrics": [REV, RET]}
UNIT = {"metrics": [REV]}
SPINE_MONTH = {"field": "time_spine.timestamp", "granularity": "month",
               "date_range": ["2025-01-01", "2025-03-31"]}


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


async def test_row_sources_of_an_inferred_query(backend: str) -> None:
    async with seed_engine(backend) as eng:
        two = await run(eng, **TWO)
        unit = await run(eng, **UNIT)
    assert (two.population, two.population_inferred, two.row_sources) == ("customers", True, ["orders", "returns"])
    assert (unit.population, unit.population_inferred, unit.row_sources) == (None, True, ["unit"])


async def test_a_dimension_only_query_lists_its_anchor(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R])
    assert (resp.population, resp.row_sources) == ("customers", ["customers"])


async def test_an_unrelated_metric_lists_the_anchor_beside_the_homes(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV, "tickets.ticket_count"])
    assert resp.row_sources == ["customers", "orders"]


async def test_explicit_population_reported(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, population="customers", **TWO)
    assert (resp.population, resp.population_inferred, resp.row_sources) == ("customers", False, ["customers"])


async def test_a_product_anchor_is_reported_by_its_parts(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, "tickets.priority"])
    assert resp.population == "customers × tickets"
    assert resp.row_sources == ["customers", "tickets"]
    assert {(r[R], r["tickets.priority"]) for r in resp.data} == {
        (region, p) for region in ("EAST", "WEST", "NORTH") for p in ("high", "low")}
    assert len(resp.data) == 6


@pytest.mark.parametrize("population, inferred", [(None, True), ("customers", False)])
async def test_spine_population_reported(backend: str, population, inferred: bool) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, **({"population": population} if population else {}), dimensions=[R],
                         time_dimensions=[SPINE_MONTH], metrics=["sum(orders.amount)"])
    assert (resp.population, resp.population_inferred, resp.row_sources) == (
        "time_spine × customers", inferred, ["customers"])


# --------------------------------------------------------------------------- #
# Execution paths
# --------------------------------------------------------------------------- #
def _reported(resp) -> tuple:
    return resp.population, resp.population_inferred, resp.row_sources


async def test_dry_run_and_explain_report_row_sources(backend: str) -> None:
    async with seed_engine(backend) as eng:
        normal = _reported(await eng.execute(q(**TWO)))
        dry = _reported(await eng.execute(q(**TWO), dry_run=True))
        explained = _reported(await eng.execute(q(**TWO), explain=True))
    assert normal == dry == explained == ("customers", True, ["orders", "returns"])


async def test_cache_hit_reports_row_sources(backend: str) -> None:
    async with seed_engine(backend) as eng:
        miss = await eng.execute(q(**TWO), cache=True)
        hit = await eng.execute(q(**TWO), cache=True)
        assert eng.cache_size == 1
    assert _reported(miss) == _reported(hit) == ("customers", True, ["orders", "returns"])


async def test_cache_twin_reports_its_own_row_sources(backend: str) -> None:
    async with seed_engine(backend) as eng:
        unit = await eng.execute(q(**UNIT), cache=True)
        anchored = await eng.execute(q(population="orders", **UNIT), cache=True)
        assert eng.cache_size == 1
    assert unit.row_sources == ["unit"]
    assert anchored.row_sources == ["orders"]


async def test_refresh_reports_row_sources() -> None:
    async with seeded("sqlite") as (eng, db_path):
        eng.cache_config = CacheConfig(refresh_keys=(("orders", "MAX(amount)"),))
        await eng.execute(q(**TWO), cache=True)
        with transaction(db_path) as con:
            con.execute("INSERT INTO orders VALUES ('O9', 'C1', 900.0, 'open', '2025-03-01')")
        result = await eng.refresh()
        assert result.refreshed
        refreshed = await eng.execute(q(**TWO), cache=True)
    assert _reported(refreshed) == ("customers", True, ["orders", "returns"])


# --------------------------------------------------------------------------- #
# REST, MCP, Python client
# --------------------------------------------------------------------------- #
async def test_rest_reports_row_sources() -> None:
    async with seed_engine("sqlite") as eng:
        with TestClient(create_app(storage=eng.storage)) as client:
            inferred = client.post("/query", json=TWO)
            anchored = client.post("/query", json={"population": "customers", **TWO})
    assert inferred.status_code == 200, inferred.text
    body = inferred.json()
    assert (body["population"], body["population_inferred"], body["row_sources"]) == (
        "customers", True, ["orders", "returns"])
    assert anchored.json()["row_sources"] == ["customers"]


def test_client_decodes_row_sources() -> None:
    resp = SlayerClient._parse_response({
        "data": [], "columns": [], "sql": "SELECT 1",
        "population": "customers", "population_inferred": True, "row_sources": ["orders", "returns"],
    })
    assert resp.row_sources == ["orders", "returns"]


async def _mcp(storage, query: dict[str, Any], fmt: str) -> str:
    server = create_mcp_server(storage=storage)
    try:
        blocks, _ = cast(Any, await server.call_tool(name="query", arguments={"query": query, "format": fmt}))
        return blocks[0].text
    finally:
        await getattr(server, "_slayer_engine").aclose()


async def test_mcp_prints_row_sources_only_when_inferred() -> None:
    async with seed_engine("sqlite") as eng:
        payload, _ = json.JSONDecoder().raw_decode(await _mcp(eng.storage, TWO, "json"))
        unit, _ = json.JSONDecoder().raw_decode(await _mcp(eng.storage, UNIT, "json"))
        anchored, _ = json.JSONDecoder().raw_decode(await _mcp(eng.storage, {"population": "customers", **TWO},
                                                               "json"))
        markdown = await _mcp(eng.storage, TWO, "markdown")
        csv = await _mcp(eng.storage, TWO, "csv")
        anchored_md = await _mcp(eng.storage, {"population": "customers", **TWO}, "markdown")
    assert payload["row_sources"] == ["orders", "returns"]
    assert payload["population"] == "customers"
    assert unit["row_sources"] == ["unit"]
    assert "row_sources" not in anchored
    assert "population" not in anchored
    row_lines = [line for line in markdown.splitlines() if "row sources" in line.lower()]
    assert row_lines
    assert "orders" in row_lines[0]
    assert "returns" in row_lines[0]
    assert any(line.startswith("#") and "row sources" in line.lower() for line in csv.splitlines())
    assert "row sources" not in anchored_md.lower()


@pytest.mark.parametrize(("query", "root"), [
    ({"metrics": [REV], "filter": "orders.status = 'completed'"}, "test.orders"),
    ({"metrics": [REV, RET]}, "test.orders"),
    (TWO, "test.customers"),
])
async def test_memory_entities_of_a_rootless_query(query: dict, root: str) -> None:
    async with seed_engine("sqlite") as eng:
        resolved = await extract_entities_from_query(SlayerQuery.model_validate(query), storage=eng.storage)
    assert resolved.canonical_forms[0] == root
