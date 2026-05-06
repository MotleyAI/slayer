"""``inspect_model`` integration + ``query`` docstring tests for DEV-1357.

The ``inspect_model`` tool gains a new ``learnings`` section (auto-pruned
when empty) listing every learning whose stored entity set overlaps the
model's own entity set (the model itself, every column, every named
measure, every custom aggregation). The ``query`` MCP tool docstring
gains a paragraph directing agents to call ``recall`` first.
"""

import os
import shutil
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
        "learnings.yaml",
        "saved_queries.yaml",
        "counters.yaml",
    ):
        p = os.path.join(storage.base_dir, f)
        if os.path.exists(p):
            os.remove(p)


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
                    name="id", sql="id", type=DataType.NUMBER, primary_key=True
                ),
                Column(name="amount", sql="amount", type=DataType.NUMBER),
                Column(name="status", sql="status", type=DataType.STRING),
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
                    name="id", sql="id", type=DataType.NUMBER, primary_key=True
                ),
                Column(name="name", sql="name", type=DataType.STRING),
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
# inspect_model — learnings section
# ---------------------------------------------------------------------------


class TestInspectModelLearningsSection:
    async def test_learnings_section_appears_when_match_exists(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        await seeded.save_learning(
            body="orders.amount in cents not dollars",
            entities=["mydb.orders.amount"],
        )
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "orders", "data_source": "mydb"},
        )
        # The section header appears.
        assert "Learnings" in result or "learnings" in result.lower()
        # The body of the matching learning is rendered.
        assert "orders.amount in cents not dollars" in result

    async def test_section_pruned_when_no_relevant_learnings(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # Save a learning that targets a different model — orders'
        # inspect must NOT show it.
        await seeded.save_learning(
            body="customer-only note",
            entities=["mydb.customers.name"],
        )
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "orders", "data_source": "mydb"},
        )
        # No "Learnings" section header, and the customer-specific note
        # text isn't anywhere in the orders response.
        assert "customer-only note" not in result

    async def test_section_pruned_when_no_learnings_at_all(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "orders", "data_source": "mydb"},
        )
        # No "Learnings" header should appear when no learnings exist.
        # (Models always render the columns section, so a generic
        # "## Learnings" check is the right shape.)
        assert "## Learnings" not in result
        assert "## learnings" not in result.lower()

    async def test_learning_against_named_measure_appears(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        await seeded.save_learning(
            body="aov measure excludes refunded orders",
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
        await seeded.save_learning(
            body="orders is the canonical revenue table",
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
        await seeded.save_learning(
            body="amount is in cents",
            entities=["mydb.orders.amount"],
        )
        # Caller asks for only the columns section — learnings should
        # not appear.
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


# ---------------------------------------------------------------------------
# query() tool docstring update
# ---------------------------------------------------------------------------


class TestQueryDocstring:
    async def test_query_docstring_mentions_recall(
        self, mcp_server
    ) -> None:
        tools = await mcp_server.list_tools()
        query_tool = next(t for t in tools if t.name == "query")
        # The §8 paragraph adds an explicit "call recall first" hint.
        assert "recall" in (query_tool.description or "").lower()
