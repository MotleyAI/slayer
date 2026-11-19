"""Schema drift drops exactly the broken edge and replays through model editing."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from mcp.types import TextContent

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import EditDatasetDelete, WholeDatasetDelete, validate_datasource
from slayer.mcp.server import create_mcp_server

from tests._dev2073_fixtures import (
    DS,
    SHIPPING_PAIRS,
    Live,
    addresses_dataset,
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
        orders = orders.model_copy(update={"fields": [c for c in orders.fields if c.name != "customer_id"]})
        for model in (addresses_dataset(), orders):
            await lv.storage.save_dataset(model)
        yield lv


async def _orders_entry(lv: Live) -> EditDatasetDelete:
    models = list((await lv.datasets()).values())
    entries = [e for e in await validate_datasource(datasource=lv.ds, datasets=models, client=lv.client()) if e.dataset_name == "orders"]
    assert len(entries) == 1
    entry = entries[0]
    assert isinstance(entry, EditDatasetDelete)
    return entry


class TestDropAddressesOneEdge:
    async def test_report_names_the_edge_and_carries_its_exact_reference(self, drifted: Live) -> None:
        entry = await _orders_entry(drifted)
        assert entry.remove.relationships == ["billing_address"]
        assert entry.remove.model_dump(mode="json")["relationship_edges"] == [
            {"to": "addresses", "name": "billing_address", "from_columns": ["billing_address_id"], "to_columns": ["id"]},
        ]
        assert "relationship:billing_address" in {r.target for r in entry.reasons}

    async def test_applying_the_drift_keeps_the_parallel_edge(self, drifted: Live) -> None:
        entry = EditDatasetDelete.model_validate_json((await _orders_entry(drifted)).model_dump_json())
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
            name="edit_dataset",
            arguments={"dataset_name": "orders", "remove": entry.remove.model_dump(mode="json")},
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
                {"population": "orders", "dimensions": [f"{prefix}{edge}.city"], "metrics": [{"metric": "count(orders.*)"}]},
            )
            await drifted.engine().save_dataset(SlayerDataset(name=name, data_source=DS, source_queries=[stage]))
        models = list((await drifted.datasets()).values())
        whole = [e for e in await validate_datasource(datasource=drifted.ds, datasets=models, client=drifted.client())
                 if isinstance(e, WholeDatasetDelete)]
        assert [e.dataset_name for e in whole] == ["by_billing"]
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


def _route_datasets() -> list[SlayerDataset]:
    return [
        SlayerDataset(
            name=table, data_source=DS, sql_table=table,
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                     DatasetField(name="email", datatype=DataType.STRING),
                     *(DatasetField(name=fk, datatype=DataType.INTEGER) for fk in fks)],
            relationships=[join_to([[fk, "id"]], target=_ROUTE_FKS[fk]) for fk in fks],
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
            for model in _route_datasets():
                await lv.storage.save_dataset(model)
            for name, dim in (("full", "subscription.customer.consumer.email"), ("short", "consumer.email")):
                stage = SlayerQuery.model_validate(
                    {"population": "invoice", "dimensions": [dim], "metrics": [{"metric": "count(invoice.*)"}]},
                )
                await lv.storage.save_dataset(SlayerDataset(name=name, data_source=DS, source_queries=[stage]))
            models = list((await lv.datasets()).values())
            entries = await validate_datasource(datasource=lv.ds, datasets=models, client=lv.client())
            assert {e.dataset_name for e in entries if isinstance(e, WholeDatasetDelete)} == removed


class TestNonParallelDropReportsTheTarget:
    async def test_sole_join_drop_reports_the_target(self) -> None:
        async with live(LIVE_WITHOUT_BILLING) as lv:
            customers = SlayerDataset(
                name="customers", data_source=DS, sql_table="customers",
                fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                         DatasetField(name="name", datatype=DataType.STRING)],
            )
            orders = SlayerDataset(
                name="orders", data_source=DS, sql_table="orders",
                fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                         DatasetField(name="customer_id", datatype=DataType.INTEGER)],
                relationships=[join_to([["customer_id", "id"]], target="customers")],
            )
            for model in (customers, orders):
                await lv.storage.save_dataset(model)
            entry = await _orders_entry(lv)
            assert entry.remove.relationships == ["customers"]
            assert "relationship:customers" in {r.target for r in entry.reasons}
