"""Schema drift drops exactly the broken edge and replays through model editing."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from mcp.types import TextContent

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import EditModelDelete, validate_datasource
from slayer.mcp.server import create_mcp_server

from tests._dev2073_fixtures import (
    DS,
    SHIPPING_PAIRS,
    Live,
    addresses_model,
    join_to,
    live,
    named_pair,
    orders_with,
    stored_joins,
)

# Live orders has lost billing_address_id; every other modelled column survives.
LIVE_WITHOUT_BILLING = """
CREATE TABLE addresses (id INTEGER PRIMARY KEY, city TEXT);
CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE orders (id INTEGER PRIMARY KEY, shipping_address_id INTEGER, legacy_addr INTEGER,
                     status TEXT);
"""


@pytest.fixture
async def drifted() -> AsyncIterator[Live]:
    async with live(LIVE_WITHOUT_BILLING) as lv:
        # Shipping first, so a first-match-by-target removal picks the wrong edge.
        orders = orders_with(*reversed(named_pair()))
        orders = orders.model_copy(update={"columns": [c for c in orders.columns if c.name != "customer_id"]})
        for model in (addresses_model(), orders):
            await lv.storage.save_model(model)
        yield lv


async def _orders_entry(lv: Live) -> EditModelDelete:
    models = list((await lv.models()).values())
    entries = [e for e in await validate_datasource(datasource=lv.ds, models=models) if e.model_name == "orders"]
    assert len(entries) == 1
    entry = entries[0]
    assert isinstance(entry, EditModelDelete)
    return entry


class TestDropAddressesOneEdge:
    async def test_report_names_the_edge_and_carries_its_exact_reference(self, drifted: Live) -> None:
        entry = await _orders_entry(drifted)
        assert entry.remove.joins == ["billing_address"]
        assert entry.remove.model_dump(mode="json")["join_edges"] == [
            {"target_model": "addresses", "name": "billing_address", "join_pairs": [["billing_address_id", "id"]]},
        ]
        assert "join:billing_address" in {r.target for r in entry.reasons}

    async def test_applying_the_drift_keeps_the_parallel_edge(self, drifted: Live) -> None:
        entry = EditModelDelete.model_validate_json((await _orders_entry(drifted)).model_dump_json())
        engine = SlayerQueryEngine(storage=drifted.storage)
        try:
            result = await engine.apply_drift_deletes([entry])
        finally:
            await engine.aclose()
        assert result.errors == []
        assert await stored_joins(drifted.storage) == [("shipping_address", SHIPPING_PAIRS)]

    async def test_json_removal_replays_through_edit_model(self, drifted: Live) -> None:
        entry = await _orders_entry(drifted)
        server = create_mcp_server(storage=drifted.storage, _seed_help=False)
        blocks, _ = await server.call_tool(
            name="edit_model",
            arguments={"model_name": "orders", "remove": entry.remove.model_dump(mode="json")},
        )
        (block,) = blocks
        assert isinstance(block, TextContent)
        assert '"success": true' in block.text
        assert await stored_joins(drifted.storage) == [("shipping_address", SHIPPING_PAIRS)]


class TestNonParallelDropReportsTheTarget:
    async def test_sole_join_drop_reports_the_target(self) -> None:
        async with live(LIVE_WITHOUT_BILLING) as lv:
            customers = SlayerModel(
                name="customers", data_source=DS, sql_table="customers",
                columns=[Column(name="id", type=DataType.INT, primary_key=True),
                         Column(name="name", type=DataType.TEXT)],
            )
            orders = SlayerModel(
                name="orders", data_source=DS, sql_table="orders",
                columns=[Column(name="id", type=DataType.INT, primary_key=True),
                         Column(name="customer_id", type=DataType.INT)],
                joins=[join_to([["customer_id", "id"]], target="customers")],
            )
            for model in (customers, orders):
                await lv.storage.save_model(model)
            entry = await _orders_entry(lv)
            assert entry.remove.joins == ["customers"]
            assert "join:customers" in {r.target for r in entry.reasons}
