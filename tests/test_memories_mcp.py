"""MCP tool tests for the unified Memory write surface (DEV-1357 v2).

Two MCP tools land on the write side:

* ``save_memory(learning, linked_entities)`` — ``linked_entities`` accepts
  either a list of entity strings (each must resolve, errors are fatal)
  or a ``SlayerQuery``/dict (entities are auto-extracted, resolution
  warnings are non-fatal, the query is persisted on the memory).
* ``forget_memory(id)`` — accepts ``int`` or its decimal string form.

Memory retrieval is part of ``search`` — see ``test_search_surfaces.py``
and ``test_search_service.py``. Tests follow the same
``_call(mcp_server, name=..., arguments=...)`` pattern as the existing
MCP test suite.
"""

import json
import os
import shutil
import sqlite3
import tempfile
from typing import Any, Generator, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery
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
    # ``embeddings.db`` is opened by ``storage._embeddings_store``; clear
    # rows in place rather than deleting the file (which would leave the
    # SidecarEmbeddingStore pointing at a now-empty path without its
    # CREATE TABLE having been run).
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
    await storage.save_datasource(
        DatasourceConfig(name="other", type="postgres", host="x")
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
                Column(
                    name="customer_id",
                    sql="customer_id",
                    type=DataType.DOUBLE,
                ),
            ],
            measures=[ModelMeasure(formula="amount:sum / *:count", name="aov")],
            aggregations=[
                Aggregation(
                    name="weighted_score",
                    formula="SUM(amount * amount) / SUM(amount)",
                ),
            ],
            joins=[
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
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
    await storage.save_model(
        SlayerModel(
            name="invoices",
            data_source="mydb",
            sql_table="invoices",
            columns=[
                Column(
                    name="id", sql="id", type=DataType.DOUBLE, primary_key=True
                ),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
    )
    await storage.set_datasource_priority(["mydb", "other"])
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


def _try_parse_json(text: str) -> Optional[dict]:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


# ---------------------------------------------------------------------------
# save_memory — list[str] entry path (current save_learning semantics)
# ---------------------------------------------------------------------------


class TestSaveMemoryEntityList:
    async def test_persists_with_resolved_entities(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="save_memory",
            arguments={
                "learning": "treat NULL is_returned as not returned",
                "linked_entities": ["mydb.orders.amount"],
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, f"non-JSON response: {result}"
        assert payload["memory_id"] == "1"
        assert payload["resolved_entities"] == ["mydb.orders.amount"]
        assert payload["warnings"] == []
        loaded = await seeded.get_memory("1")
        assert loaded.learning == "treat NULL is_returned as not returned"
        assert loaded.query is None

    async def test_canonicalizes_inputs(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="save_memory",
            arguments={
                "learning": "n",
                "linked_entities": [
                    "orders.amount:sum",
                    "mydb.orders.amount",
                    "status",
                ],
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert sorted(payload["resolved_entities"]) == [
            "mydb.orders.amount",
            "mydb.orders.status",
        ]

    async def test_empty_linked_entities_errors(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="save_memory",
            arguments={"learning": "x", "linked_entities": []},
        )
        assert "linked_entities" in result.lower() or "empty" in result.lower()
        assert await seeded.list_memories() == []

    async def test_resolution_error_does_not_persist(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # ``amount`` is ambiguous (orders + invoices); list-string path
        # propagates the error verbatim, no row written.
        result = await _call(
            mcp_server,
            name="save_memory",
            arguments={"learning": "x", "linked_entities": ["amount"]},
        )
        assert "ambiguous" in result.lower() or "amount" in result.lower()
        assert await seeded.list_memories() == []

    async def test_id_monotonic(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        for expected in ("1", "2", "3"):
            result = await _call(
                mcp_server,
                name="save_memory",
                arguments={
                    "learning": "x",
                    "linked_entities": ["mydb.orders.amount"],
                },
            )
            payload = _try_parse_json(result)
            assert payload is not None
            assert payload["memory_id"] == expected


# ---------------------------------------------------------------------------
# save_memory — SlayerQuery / dict entry path (current save_query semantics)
# ---------------------------------------------------------------------------


class TestSaveMemoryQuery:
    async def test_persists_with_extracted_entities_and_query(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="amount:sum")],
            filters=["status = 'paid'"],
        )
        result = await _call(
            mcp_server,
            name="save_memory",
            arguments={
                "learning": "Paid revenue by status",
                "linked_entities": query.model_dump(mode="json"),
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert payload["memory_id"] == "1"
        assert "mydb.orders" in payload["resolved_entities"]
        assert "mydb.orders.status" in payload["resolved_entities"]
        assert "mydb.orders.amount" in payload["resolved_entities"]
        loaded = await seeded.get_memory("1")
        assert isinstance(loaded.query, SlayerQuery)
        assert loaded.query.source_model == "orders"
        assert loaded.learning == "Paid revenue by status"

    async def test_dict_input_coerces_to_query(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        query_dict = {
            "source_model": "orders",
            "measures": [{"formula": "*:count"}],
        }
        result = await _call(
            mcp_server,
            name="save_memory",
            arguments={
                "learning": "Order count",
                "linked_entities": query_dict,
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert payload["memory_id"] == "1"
        loaded = await seeded.get_memory("1")
        assert isinstance(loaded.query, SlayerQuery)

    async def test_source_model_always_tagged(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        query = SlayerQuery(
            source_model="customers",
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await _call(
            mcp_server,
            name="save_memory",
            arguments={
                "learning": "Customer count",
                "linked_entities": query.model_dump(mode="json"),
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert "mydb.customers" in payload["resolved_entities"]


# ---------------------------------------------------------------------------
# forget_memory
# ---------------------------------------------------------------------------


class TestForgetMemory:
    async def test_deletes_existing_memory(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        memory = await seeded.save_memory(
            learning="x", entities=["mydb.orders"]
        )
        result = await _call(
            mcp_server,
            name="forget_memory",
            arguments={"id": memory.id},
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert payload["deleted_id"] == memory.id
        assert await seeded.list_memories() == []

    async def test_accepts_decimal_string_id(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        memory = await seeded.save_memory(
            learning="x", entities=["mydb.orders"]
        )
        result = await _call(
            mcp_server,
            name="forget_memory",
            arguments={"id": str(memory.id)},
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert payload["deleted_id"] == memory.id

    async def test_unknown_id_errors(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="forget_memory",
            arguments={"id": 999},
        )
        assert "not found" in result.lower() or "999" in result

    async def test_invalid_id_format_errors(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="forget_memory",
            arguments={"id": "not_an_int"},
        )
        # Tool must reject non-numeric strings without crashing.
        assert "id" in result.lower() or "int" in result.lower()


# ---------------------------------------------------------------------------
# Tool registration smoke
# ---------------------------------------------------------------------------


class TestToolRegistration:
    async def test_tools_registered(self, mcp_server) -> None:
        tools = await mcp_server.list_tools()
        names = {t.name for t in tools}
        assert {"save_memory", "forget_memory"}.issubset(names)
        # Memory retrieval is now part of `search`.
        assert "search" in names
        # Old names are gone.
        assert "save_learning" not in names
        assert "save_query" not in names
        assert "delete_learning_or_query" not in names
        assert "recall" not in names
        assert "recall_memories" not in names
