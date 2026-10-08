"""Join mutations address one edge: MCP ``edit_model`` and engine ``edit_model_remove``."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

import pytest

from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev2073_fixtures import (
    BILLING_PAIRS,
    DS,
    SHIPPING_PAIRS,
    addresses_model,
    model_store,
    named_pair,
    orders_with,
    stored_joins,
    unnamed_pair,
)


@pytest.fixture
async def storage() -> AsyncIterator[YAMLStorage]:
    async with model_store(addresses_model()) as store:
        yield store


@pytest.fixture
def mcp_server(storage: YAMLStorage):
    return create_mcp_server(storage=storage, _seed_help=False)


@pytest.fixture
async def engine(storage: YAMLStorage) -> AsyncIterator[SlayerQueryEngine]:
    eng = SlayerQueryEngine(storage=storage)
    yield eng
    await eng.aclose()


async def _edit(mcp_server, **arguments) -> str:
    blocks, _ = await mcp_server.call_tool(name="edit_model", arguments={"model_name": "orders", **arguments})
    return blocks[0].text


def _succeeded(text: str) -> bool:
    try:
        return json.loads(text).get("success") is True
    except json.JSONDecodeError:
        return False


class TestMcpEditModelAddressesOneEdge:
    async def test_remove_one_of_two_named_edges(self, mcp_server, storage) -> None:
        await storage.save_model(orders_with(*named_pair()))
        assert _succeeded(await _edit(mcp_server, remove={"joins": ["billing_address"]}))
        assert await stored_joins(storage) == [("shipping_address", SHIPPING_PAIRS)]

    async def test_ambiguous_target_reference_fails_listing_candidates(self, mcp_server, storage) -> None:
        await storage.save_model(orders_with(*unnamed_pair()))
        text = await _edit(mcp_server, remove={"joins": ["addresses"]})
        assert not _succeeded(text)
        assert "billing_address_id" in text and "shipping_address_id" in text
        assert len(await stored_joins(storage)) == 2

    async def test_upsert_names_the_matching_unnamed_edge(self, mcp_server, storage) -> None:
        await storage.save_model(orders_with(*unnamed_pair()))
        spec = {"target_model": "addresses", "join_pairs": SHIPPING_PAIRS, "name": "shipping_address"}
        assert _succeeded(await _edit(mcp_server, joins=[spec]))
        assert await stored_joins(storage) == [(None, BILLING_PAIRS), ("shipping_address", SHIPPING_PAIRS)]

    async def test_upsert_matches_by_name_before_pairs(self, mcp_server, storage) -> None:
        await storage.save_model(orders_with(*reversed(named_pair())))
        # The name selects billing; the pairs alone would select shipping.
        spec = {"target_model": "addresses", "join_pairs": SHIPPING_PAIRS, "name": "billing_address"}
        assert _succeeded(await _edit(mcp_server, joins=[spec]))
        assert await stored_joins(storage) == [
            ("shipping_address", SHIPPING_PAIRS), ("billing_address", SHIPPING_PAIRS),
        ]

    async def test_exact_reference_removes_exactly_one_edge(self, mcp_server, storage) -> None:
        await storage.save_model(orders_with(*unnamed_pair()))
        ref = {"target_model": "addresses", "join_pairs": SHIPPING_PAIRS}
        assert _succeeded(await _edit(mcp_server, remove={"join_edges": [ref]}))
        assert await stored_joins(storage) == [(None, BILLING_PAIRS)]

    async def test_unnamed_join_to_an_already_parallel_target_is_rejected(self, mcp_server, storage) -> None:
        await storage.save_model(orders_with(*named_pair()))
        text = await _edit(mcp_server, joins=[{"target_model": "addresses", "join_pairs": [["legacy_addr", "id"]]}])
        assert not _succeeded(text)
        assert "name" in text
        assert len(await stored_joins(storage)) == 2


class TestEngineRemovalAddressesOneEdge:
    async def test_remove_by_edge_name(self, engine, storage) -> None:
        await storage.save_model(orders_with(*named_pair()))
        await engine.edit_model_remove(model_name="orders", data_source=DS, remove_joins=["billing_address"])
        assert await stored_joins(storage) == [("shipping_address", SHIPPING_PAIRS)]

    async def test_ambiguous_target_reference_raises(self, engine, storage) -> None:
        await storage.save_model(orders_with(*unnamed_pair()))
        with pytest.raises(ValueError, match="shipping_address_id"):
            await engine.edit_model_remove(model_name="orders", data_source=DS, remove_joins=["addresses"])
        assert len(await stored_joins(storage)) == 2
