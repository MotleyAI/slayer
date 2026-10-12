"""Schema drift drops exactly the broken edge and replays through model editing."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from mcp.types import TextContent

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import EditModelDelete, WholeModelDelete, validate_datasource
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


class TestQueryBackedCascadeFollowsTheEdge:
    @pytest.mark.parametrize("prefix", ["", "orders."])
    async def test_only_a_stage_walking_the_dropped_edge_is_whole_dropped(self, drifted: Live, prefix: str) -> None:
        for name, edge in (("by_shipping", "shipping_address"), ("by_billing", "billing_address")):
            stage = SlayerQuery.model_validate(
                {"source_model": "orders", "dimensions": [f"{prefix}{edge}.city"], "measures": [{"formula": "*:count"}]},
            )
            await drifted.storage.save_model(SlayerModel(name=name, data_source=DS, source_queries=[stage]))
        models = list((await drifted.models()).values())
        whole = [e for e in await validate_datasource(datasource=drifted.ds, models=models)
                 if isinstance(e, WholeModelDelete)]
        assert [e.model_name for e in whole] == ["by_billing"]
        assert "'billing_address' via 'orders'" in whole[0].reasons[0].reason


_ROUTE_TABLES = {
    "invoice": ["subscription_id", "promo_id"],
    "subscription": ["customer_id"],
    "customer": ["consumer_id"],
    "consumer": [],
    "promo": [],
}
_ROUTE_FKS = {"subscription_id": "subscription", "customer_id": "customer", "consumer_id": "consumer",
              "promo_id": "promo"}


def _route_models() -> list[SlayerModel]:
    return [
        SlayerModel(
            name=table, data_source=DS, sql_table=table,
            columns=[Column(name="id", type=DataType.INT, primary_key=True),
                     Column(name="email", type=DataType.TEXT),
                     *(Column(name=fk, type=DataType.INT) for fk in fks)],
            joins=[join_to([[fk, "id"]], target=_ROUTE_FKS[fk]) for fk in fks],
        )
        for table, fks in _ROUTE_TABLES.items()
    ]


def _live_without(dropped_fk: str) -> str:
    return "\n".join(
        f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, email TEXT"
        + "".join(f", {fk} INTEGER" for fk in fks if fk != dropped_fk) + ");"
        for table, fks in _ROUTE_TABLES.items()
    )


class TestEveryHopOfTheRouteCounts:
    @pytest.mark.parametrize(("dropped_fk", "removed"), [
        ("subscription_id", {"full", "short"}),
        ("customer_id", {"full", "short"}),
        ("consumer_id", {"full", "short"}),
        ("promo_id", set()),
    ])
    async def test_stage_cascades_on_any_hop_it_walks(self, dropped_fk: str, removed: set[str]) -> None:
        async with live(_live_without(dropped_fk)) as lv:
            for model in _route_models():
                await lv.storage.save_model(model)
            for name, dim in (("full", "subscription.customer.consumer.email"), ("short", "consumer.email")):
                stage = SlayerQuery.model_validate(
                    {"source_model": "invoice", "dimensions": [dim], "measures": [{"formula": "*:count"}]},
                )
                await lv.storage.save_model(SlayerModel(name=name, data_source=DS, source_queries=[stage]))
            models = list((await lv.models()).values())
            entries = await validate_datasource(datasource=lv.ds, models=models)
            assert {e.model_name for e in entries if isinstance(e, WholeModelDelete)} == removed


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
