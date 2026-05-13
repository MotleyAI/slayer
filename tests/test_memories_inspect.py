"""``inspect_model`` integration + ``query`` docstring tests for DEV-1357 v2.

The ``inspect_model`` tool gains a ``Learnings`` section that surfaces
**only memories where ``query is None``** — query-bearing memories appear
only via ``search`` (in the ``example_queries`` bucket). The section is
auto-pruned when no matching learning-shaped memory exists. The
``query`` MCP tool docstring gains a paragraph directing agents to call
``search`` first.
"""

import os
import shutil
import sqlite3
import tempfile
from typing import Any, Generator, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture(scope="session")
def _shared_storage() -> Generator[YAMLStorage, None, None]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture(scope="session")
def _shared_mcp_server(_shared_storage: YAMLStorage):
    return create_mcp_server(storage=_shared_storage)


def _reset_storage(storage: YAMLStorage) -> None:
    for sub in ("models", "datasources"):
        d = os.path.join(storage.base_dir, sub)
        if os.path.isdir(d):
            for entry in os.listdir(d):
                path = os.path.join(d, entry)
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
    for f in (
        "priority.yaml",
        "memories.yaml",
        "embeddings.yaml.legacy",
        "counters.yaml.legacy",
    ):
        p = os.path.join(storage.base_dir, f)
        if os.path.exists(p):
            os.remove(p)
    # See _reset_storage in test_memories_mcp.py — clear rows in place
    # rather than deleting the SidecarEmbeddingStore's open db file.
    emb_path = os.path.join(storage.base_dir, "embeddings.db")
    if os.path.exists(emb_path):
        with sqlite3.connect(emb_path) as conn:
            conn.execute("DELETE FROM embeddings")


@pytest.fixture
def storage(_shared_storage: YAMLStorage) -> YAMLStorage:
    _reset_storage(_shared_storage)
    return _shared_storage


@pytest.fixture
def mcp_server(_shared_mcp_server, storage: YAMLStorage):
    return _shared_mcp_server


@pytest.fixture
async def seeded(storage: YAMLStorage) -> YAMLStorage:
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="postgres", host="x")
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            data_source="mydb",
            sql_table="orders",
            columns=[
                Column(
                    name="id", sql="id", type=DataType.DOUBLE, primary_key=True
                ),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
            measures=[
                ModelMeasure(formula="amount:sum / *:count", name="aov"),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="customers",
            data_source="mydb",
            sql_table="customers",
            columns=[
                Column(
                    name="id", sql="id", type=DataType.DOUBLE, primary_key=True
                ),
                Column(name="name", sql="name", type=DataType.TEXT),
            ],
        )
    )
    await storage.set_datasource_priority(["mydb"])
    return storage


async def _call(
    mcp_server,
    *,
    name: str,
    arguments: Optional[dict[str, Any]] = None,
) -> str:
    content_blocks, _ = await mcp_server.call_tool(
        name=name, arguments=arguments or {}
    )
    return content_blocks[0].text


# ---------------------------------------------------------------------------
# inspect_model — Learnings section (only memories where query is None)
# ---------------------------------------------------------------------------


class TestInspectModelLearningsSection:
    async def test_learnings_section_appears_when_match_exists(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        await seeded.save_memory(
            learning="orders.amount in cents not dollars",
            entities=["mydb.orders.amount"],
        )
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "orders", "data_source": "mydb"},
        )
        assert "Learnings" in result or "learnings" in result.lower()
        assert "orders.amount in cents not dollars" in result

    async def test_section_pruned_when_only_query_bearing_memories_match(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # A memory with a query attached must NOT surface in inspect_model.
        await seeded.save_memory(
            learning="example query",
            entities=["mydb.orders.amount"],
            query=SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="amount:sum")],
            ),
        )
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "orders", "data_source": "mydb"},
        )
        assert "## Learnings" not in result
        assert "example query" not in result

    async def test_section_pruned_when_no_relevant_memories(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        await seeded.save_memory(
            learning="customer-only note",
            entities=["mydb.customers.name"],
        )
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "orders", "data_source": "mydb"},
        )
        assert "customer-only note" not in result
        # Pin the section header itself — an empty "## Learnings"
        # heading would still pass the body check above.
        assert "## Learnings" not in result

    async def test_section_pruned_when_no_memories_at_all(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "orders", "data_source": "mydb"},
        )
        assert "## Learnings" not in result
        assert "## learnings" not in result.lower()

    async def test_learning_against_named_measure_appears(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        await seeded.save_memory(
            learning="aov measure excludes refunded orders",
            entities=["mydb.orders.aov"],
        )
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "orders", "data_source": "mydb"},
        )
        assert "aov measure excludes refunded orders" in result

    async def test_learning_against_model_itself_appears(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        await seeded.save_memory(
            learning="orders is the canonical revenue table",
            entities=["mydb.orders"],
        )
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "orders", "data_source": "mydb"},
        )
        assert "orders is the canonical revenue table" in result

    async def test_section_excluded_when_not_in_sections(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        await seeded.save_memory(
            learning="amount is in cents",
            entities=["mydb.orders.amount"],
        )
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={
                "model_name": "orders",
                "data_source": "mydb",
                "sections": ["columns"],
            },
        )
        assert "amount is in cents" not in result
        assert "## Learnings" not in result

    async def test_json_output_uses_learning_field(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        """``inspect_model(format='json', sections=['learnings'])`` must
        emit ``learning`` (the Memory field name) — not the legacy
        ``body`` alias, which would AttributeError as soon as a memory
        matches."""
        import json

        await seeded.save_memory(
            learning="amount is in cents",
            entities=["mydb.orders.amount"],
        )
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={
                "model_name": "orders",
                "data_source": "mydb",
                "format": "json",
                "sections": ["learnings"],
            },
        )
        payload = json.loads(result)
        assert payload["learnings"], payload
        first = payload["learnings"][0]
        assert first["learning"] == "amount is in cents"
        assert "body" not in first


# ---------------------------------------------------------------------------
# query() tool docstring update
# ---------------------------------------------------------------------------


class TestQueryDocstring:
    async def test_query_docstring_mentions_search(
        self, mcp_server
    ) -> None:
        tools = await mcp_server.list_tools()
        query_tool = next(t for t in tools if t.name == "query")
        # The docstring directs agents to call `search` first.
        assert "search" in (query_tool.description or "")
        assert "recall_memories" not in (query_tool.description or "")
