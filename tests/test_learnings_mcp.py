"""MCP tool tests for Learnings + saved queries (DEV-1357).

Covers the four tools introduced by DEV-1357:

* ``save_learning`` — record a free-form note keyed by canonical entities.
* ``save_query`` — persist a ``SlayerQuery`` (or run-by-name model) with
  a description and the entities it references.
* ``delete_learning_or_query`` — remove either kind by ID.
* ``recall`` — look up learnings + saved queries by entity overlap.

Tests call the tools through the same ``_call(mcp_server, name=...,
arguments=...)`` helper used by ``test_mcp_server.py``. Tools return
JSON-formatted Pydantic responses on success; on failure the returned
string carries the human-readable error (matching the existing MCP
convention of never raising back to the agent).
"""

import json
import os
import shutil
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


# Mirror the session-scoped MCP-server fixture from ``test_mcp_server.py``
# — FastMCP construction is expensive (~35 ms per instance), and the
# storage directory is reset between tests via ``_reset_storage``.
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
    """A two-datasource layout with the same shape as the resolver test
    fixture, so MCP tool tests can exercise resolution end-to-end."""
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
                    name="id", sql="id", type=DataType.NUMBER, primary_key=True
                ),
                Column(name="amount", sql="amount", type=DataType.NUMBER),
                Column(name="status", sql="status", type=DataType.STRING),
                Column(
                    name="customer_id",
                    sql="customer_id",
                    type=DataType.NUMBER,
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
                    name="id", sql="id", type=DataType.NUMBER, primary_key=True
                ),
                Column(name="name", sql="name", type=DataType.STRING),
            ],
        )
    )
    # An ambiguous-bare-column setup: ``amount`` lives on both orders and
    # invoices in mydb — bare resolution must error out (Case B1).
    await storage.save_model(
        SlayerModel(
            name="invoices",
            data_source="mydb",
            sql_table="invoices",
            columns=[
                Column(
                    name="id", sql="id", type=DataType.NUMBER, primary_key=True
                ),
                Column(name="amount", sql="amount", type=DataType.NUMBER),
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
# save_learning
# ---------------------------------------------------------------------------


class TestSaveLearning:
    async def test_persists_with_resolved_entities(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="save_learning",
            arguments={
                "learning": "treat NULL is_returned as not returned",
                "linked_entities": ["mydb.orders.amount"],
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, f"non-JSON response: {result}"
        assert payload["learning_id"] == "L1"
        assert payload["resolved_entities"] == ["mydb.orders.amount"]
        assert payload["warnings"] == []
        # Verify storage actually has the learning.
        loaded = await seeded.get_learning("L1")
        assert loaded.body == "treat NULL is_returned as not returned"

    async def test_canonicalizes_inputs(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # ``orders.amount:sum`` and ``mydb.orders.amount`` and bare
        # ``status`` (after priority resolution) all canonicalise.
        result = await _call(
            mcp_server,
            name="save_learning",
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
        # Aggregation is stripped; duplicate canonicalised refs are
        # deduplicated.
        assert sorted(payload["resolved_entities"]) == [
            "mydb.orders.amount",
            "mydb.orders.status",
        ]

    async def test_empty_linked_entities_errors(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="save_learning",
            arguments={"learning": "x", "linked_entities": []},
        )
        # Tool surfaces the error verbatim; no payload written.
        assert "linked_entities" in result.lower() or "empty" in result.lower()
        assert await seeded.list_learnings() == []

    async def test_resolution_error_does_not_persist(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="save_learning",
            arguments={
                "learning": "x",
                "linked_entities": ["amount"],  # ambiguous bare column
            },
        )
        # Caller sees the resolver's error message verbatim.
        assert "ambiguous" in result.lower() or "amount" in result.lower()
        assert await seeded.list_learnings() == []

    async def test_warnings_returned(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # Saving a learning against ``other`` (a datasource that's also
        # NOT a model — no Case D fires here, since ``other`` isn't a
        # model name in any datasource). Use ``mydb`` which IS a model
        # in datasource other to trigger Case D.
        await seeded.save_model(
            SlayerModel(
                name="mydb",
                data_source="other",
                sql_table="x",
                columns=[
                    Column(
                        name="id",
                        sql="id",
                        type=DataType.NUMBER,
                        primary_key=True,
                    )
                ],
            )
        )
        result = await _call(
            mcp_server,
            name="save_learning",
            arguments={
                "learning": "n",
                "linked_entities": ["mydb"],
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert payload["warnings"]
        assert any("datasource" in w.lower() for w in payload["warnings"])

    async def test_id_monotonic(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        for expected in ("L1", "L2", "L3"):
            result = await _call(
                mcp_server,
                name="save_learning",
                arguments={
                    "learning": "x",
                    "linked_entities": ["mydb.orders.amount"],
                },
            )
            payload = _try_parse_json(result)
            assert payload is not None
            assert payload["learning_id"] == expected


# ---------------------------------------------------------------------------
# save_query
# ---------------------------------------------------------------------------


class TestSaveQuery:
    async def test_persists_with_extracted_entities(
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
            name="save_query",
            arguments={
                "query": query.model_dump(mode="json"),
                "description": "Paid revenue by status",
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert payload["query_id"] == "Q1"
        assert "mydb.orders" in payload["resolved_entities"]
        assert "mydb.orders.status" in payload["resolved_entities"]
        assert "mydb.orders.amount" in payload["resolved_entities"]
        # Verify storage round-trip preserves the SlayerQuery shape.
        loaded = await seeded.get_saved_query("Q1")
        assert isinstance(loaded.query, SlayerQuery)
        assert loaded.query.source_model == "orders"
        assert loaded.description == "Paid revenue by status"

    async def test_dict_input_coerces_to_query(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        query_dict = {
            "source_model": "orders",
            "measures": [{"formula": "*:count"}],
        }
        result = await _call(
            mcp_server,
            name="save_query",
            arguments={
                "query": query_dict,
                "description": "Order count",
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert payload["query_id"] == "Q1"
        loaded = await seeded.get_saved_query("Q1")
        assert isinstance(loaded.query, SlayerQuery)

    async def test_string_run_by_name_materialises(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # Build a query-backed model whose backing query references
        # mydb.orders.amount. ``save_query("qb_model")`` should
        # materialise that backing query into a SlayerQuery and persist
        # it.
        from slayer.engine.query_engine import SlayerQueryEngine

        engine = SlayerQueryEngine(storage=seeded)
        await engine.create_model_from_query(
            query=SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="amount:sum")],
            ),
            name="qb_orders",
            save=True,
        )
        result = await _call(
            mcp_server,
            name="save_query",
            arguments={
                "query": "qb_orders",
                "description": "qb",
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        loaded = await seeded.get_saved_query(payload["query_id"])
        # The persisted record holds a fully-materialised SlayerQuery,
        # not the original "qb_orders" string.
        assert isinstance(loaded.query, SlayerQuery)

    async def test_source_model_always_tagged(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # No fields explicitly reference the source model, but it's
        # still tagged.
        query = SlayerQuery(
            source_model="customers",
            measures=[ModelMeasure(formula="*:count")],
        )
        result = await _call(
            mcp_server,
            name="save_query",
            arguments={
                "query": query.model_dump(mode="json"),
                "description": "Customer count",
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert "mydb.customers" in payload["resolved_entities"]


# ---------------------------------------------------------------------------
# delete_learning_or_query
# ---------------------------------------------------------------------------


class TestDeleteLearningOrQuery:
    async def test_deletes_learning(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        learning = await seeded.save_learning(
            body="x", entities=["mydb.orders"]
        )
        result = await _call(
            mcp_server,
            name="delete_learning_or_query",
            arguments={"id": learning.id},
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert payload["deleted_id"] == learning.id
        assert payload["kind"] == "learning"
        assert await seeded.list_learnings() == []

    async def test_deletes_saved_query(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        sq = await seeded.save_saved_query(
            query=SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="*:count")],
            ),
            description="d",
            entities=["mydb.orders"],
        )
        result = await _call(
            mcp_server,
            name="delete_learning_or_query",
            arguments={"id": sq.id},
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert payload["deleted_id"] == sq.id
        assert payload["kind"] == "query"
        assert await seeded.list_saved_queries() == []

    async def test_unknown_id_errors(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="delete_learning_or_query",
            arguments={"id": "L999"},
        )
        # No JSON payload — the tool surfaces a friendly error string.
        assert "not found" in result.lower() or "L999" in result

    async def test_invalid_id_format_errors(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="delete_learning_or_query",
            arguments={"id": "not_an_id"},
        )
        assert (
            "id" in result.lower()
            or "format" in result.lower()
            or "L<" in result
        )


# ---------------------------------------------------------------------------
# recall
# ---------------------------------------------------------------------------


class TestRecall:
    async def test_requires_at_least_one_input(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="recall",
            arguments={},
        )
        assert "entities" in result.lower() or "query" in result.lower()

    async def test_empty_entities_treated_as_none_with_query(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # entity_search treats entities=[] like entities=None — the
        # `query` arg suffices on its own.
        await seeded.save_learning(body="A", entities=["mydb.orders"])
        result = await _call(
            mcp_server,
            name="recall",
            arguments={
                "entities": [],
                "query": {
                    "source_model": "orders",
                    "measures": [{"formula": "*:count"}],
                },
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert any(
            hit["body"] == "A" for hit in payload["learnings"]
        )

    async def test_ranks_by_intersection_size(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # Two learnings with different overlaps to the input entity set.
        await seeded.save_learning(
            body="weak",
            entities=["mydb.orders"],
        )
        await seeded.save_learning(
            body="strong",
            entities=["mydb.orders", "mydb.orders.amount"],
        )
        result = await _call(
            mcp_server,
            name="recall",
            arguments={
                "entities": ["mydb.orders", "mydb.orders.amount"],
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        # Strong overlap (2) ranks above weak overlap (1).
        bodies = [hit["body"] for hit in payload["learnings"]]
        assert bodies[:2] == ["strong", "weak"]

    async def test_max_queries_default_is_two(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # Save 3 saved queries that all match.
        for _ in range(3):
            await seeded.save_saved_query(
                query=SlayerQuery(
                    source_model="orders",
                    measures=[ModelMeasure(formula="*:count")],
                ),
                description="d",
                entities=["mydb.orders"],
            )
        result = await _call(
            mcp_server,
            name="recall",
            arguments={"entities": ["mydb.orders"]},
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert len(payload["queries"]) == 2

    async def test_max_learnings_none_returns_all(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        for _ in range(5):
            await seeded.save_learning(body="x", entities=["mydb.orders"])
        result = await _call(
            mcp_server,
            name="recall",
            arguments={
                "entities": ["mydb.orders"],
                "max_learnings": None,
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert len(payload["learnings"]) == 5

    async def test_query_arg_extracts_entities(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        await seeded.save_learning(
            body="amount-related",
            entities=["mydb.orders.amount"],
        )
        await seeded.save_learning(
            body="status-related",
            entities=["mydb.orders.status"],
        )
        result = await _call(
            mcp_server,
            name="recall",
            arguments={
                "query": {
                    "source_model": "orders",
                    "measures": [{"formula": "amount:sum"}],
                },
            },
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        bodies = [hit["body"] for hit in payload["learnings"]]
        assert "amount-related" in bodies
        # status-related has no overlap → excluded.
        assert "status-related" not in bodies

    async def test_resolved_input_entities_returned(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="recall",
            arguments={"entities": ["orders.amount"]},
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert "mydb.orders.amount" in payload["resolved_input_entities"]

    async def test_resolution_failure_is_fatal(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        result = await _call(
            mcp_server,
            name="recall",
            arguments={"entities": ["amount"]},  # ambiguous
        )
        assert (
            "ambiguous" in result.lower() or "amount" in result.lower()
        )

    async def test_excludes_rows_with_empty_stored_entity_sets(
        self, mcp_server, seeded: YAMLStorage
    ) -> None:
        # Direct backend write so we can plant a Learning with no
        # entities (theoretically possible if a saved query references
        # nothing — though save_learning rejects empty inputs).
        from slayer.learnings.models import Learning

        await seeded._save_learning_row(
            Learning(id="L99", body="orphan", entities=[])
        )
        result = await _call(
            mcp_server,
            name="recall",
            arguments={"entities": ["mydb.orders"]},
        )
        payload = _try_parse_json(result)
        assert payload is not None, result
        assert all(
            hit["body"] != "orphan" for hit in payload["learnings"]
        )


# ---------------------------------------------------------------------------
# Tool registration smoke
# ---------------------------------------------------------------------------


class TestToolRegistration:
    async def test_all_four_tools_registered(self, mcp_server) -> None:
        tools = await mcp_server.list_tools()
        names = {t.name for t in tools}
        assert {
            "save_learning",
            "save_query",
            "delete_learning_or_query",
            "recall",
        }.issubset(names)
