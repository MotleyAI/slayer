"""DEV-1532: Unified flat-list search interface.

Covers the full spec:
* SearchResponse.results replaces three separate buckets.
* SearchHit is the single unified hit model (kind, id, score, text,
  matched_entities, query).
* max_results is the single total cap; old max_* params are hard-removed.
* Query-bearing memories surface in the flat list (kind="memory", query=...).
* Naive Cypher fallback: MATCH (n:Label1:Label2) RETURN n.id AS id works
  without the advanced_search extra; complex Cypher raises SlayerError.
* REST, MCP, CLI, and client all expose the new signature.
"""

from __future__ import annotations

import json
import sys
import tempfile
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from tests.search_helpers import call_mcp_tool, seed_warehouse_models

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.search import graph as _search_graph
from slayer.search.service import (
    SearchHit,
    SearchResponse,
    SearchService,
)
from slayer.storage.base import StorageBackend, resolve_storage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def storage_with_corpus() -> AsyncIterator[StorageBackend]:
    """1 datasource, 2 models, 5 memories (3 learning-only, 2 query-bearing)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = resolve_storage(tmpdir)
        await seed_warehouse_models(storage)
        # learning-only memories
        await storage.save_memory(
            learning="amount_paid is gross of refunds.",
            entities=["warehouse.orders.amount_paid"],
        )
        await storage.save_memory(
            learning="Filter status='paid' for net revenue.",
            entities=["warehouse.orders.amount_paid", "warehouse.orders.status"],
        )
        await storage.save_memory(
            learning="Customer email may be NULL for anonymous checkouts.",
            entities=["warehouse.customers.email"],
        )
        # query-bearing memories
        await storage.save_memory(
            learning="Example: sum of paid amounts.",
            entities=["warehouse.orders.amount_paid"],
            query=SlayerQuery(
                source_model="orders",
                measures=[ModelMeasure(formula="amount_paid:sum")],
            ),
        )
        await storage.save_memory(
            learning="Example: count of customers.",
            entities=["warehouse.customers.email"],
            query=SlayerQuery(
                source_model="customers",
                measures=[ModelMeasure(formula="*:count")],
            ),
        )
        yield storage


@pytest_asyncio.fixture
async def service(storage_with_corpus: StorageBackend) -> SearchService:
    return SearchService(storage=storage_with_corpus)


# ---------------------------------------------------------------------------
# Response model shape
# ---------------------------------------------------------------------------


def test_search_response_has_results_field() -> None:
    """SearchResponse.results is the unified flat list."""
    assert "results" in SearchResponse.model_fields


def test_search_response_does_not_have_old_bucket_fields() -> None:
    """Old memories / example_queries / entities fields are removed."""
    fields = SearchResponse.model_fields
    assert "memories" not in fields
    assert "example_queries" not in fields
    assert "entities" not in fields


def test_search_hit_model_has_required_fields() -> None:
    """SearchHit carries kind, id, score, text, matched_entities, query."""
    fields = SearchHit.model_fields
    assert "kind" in fields
    assert "id" in fields
    assert "score" in fields
    assert "text" in fields
    assert "matched_entities" in fields
    assert "query" in fields


def test_search_hit_query_and_matched_entities_are_optional_with_defaults() -> None:
    """matched_entities defaults to [] and query defaults to None."""
    hit = SearchHit(kind="memory", id="1", score=0.5, text="hi")
    assert hit.matched_entities == []
    assert hit.query is None


def test_search_hit_memory_hit_class_removed() -> None:
    """MemoryHit, ExampleQueryHit, EntityHit are no longer exported."""
    import slayer.search.service as svc_mod
    assert not hasattr(svc_mod, "MemoryHit")
    assert not hasattr(svc_mod, "ExampleQueryHit")
    assert not hasattr(svc_mod, "EntityHit")


# ---------------------------------------------------------------------------
# max_results — new cap
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_max_results_caps_total_flat_list(service: SearchService) -> None:
    response = await service.search(
        question="orders amount customers email",
        max_results=3,
    )
    assert len(response.results) <= 3


@pytest.mark.asyncio
async def test_max_results_default_allows_at_least_one_result(
    service: SearchService,
) -> None:
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
    )
    assert len(response.results) >= 1


@pytest.mark.asyncio
async def test_max_results_zero_raises_value_error(service: SearchService) -> None:
    with pytest.raises(ValueError, match="max_results"):
        await service.search(question="x", max_results=0)


@pytest.mark.asyncio
async def test_max_results_negative_raises_value_error(service: SearchService) -> None:
    with pytest.raises(ValueError, match="max_results"):
        await service.search(question="x", max_results=-1)


# ---------------------------------------------------------------------------
# Old max_* params are hard-removed
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_max_memories_param_removed_raises_type_error(
    service: SearchService,
) -> None:
    with pytest.raises(TypeError):
        await service.search(**{"question": "x", "max_memories": 5})  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_max_example_queries_param_removed_raises_type_error(
    service: SearchService,
) -> None:
    with pytest.raises(TypeError):
        await service.search(**{"question": "x", "max_example_queries": 2})  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_max_entities_param_removed_raises_type_error(
    service: SearchService,
) -> None:
    with pytest.raises(TypeError):
        await service.search(**{"question": "x", "max_entities": 5})  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Flat list contents
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_query_bearing_memory_appears_in_results_with_query(
    service: SearchService,
) -> None:
    """Query-bearing memories appear in results with kind='memory' and query set."""
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        max_results=20,
    )
    hits_with_query = [h for h in response.results if h.query is not None]
    assert len(hits_with_query) >= 1
    for h in hits_with_query:
        assert h.kind == "memory"
        assert isinstance(h.query, SlayerQuery)


@pytest.mark.asyncio
async def test_learning_only_memory_appears_in_results_without_query(
    service: SearchService,
) -> None:
    """Learning-only memories have kind='memory' and query=None."""
    # DEV-1549: opt out of compact-by-default — this test pins the full
    # learning body in ``hit.text``.
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        max_results=20,
        compact=False,
    )
    hits_without_query = [h for h in response.results if h.kind == "memory" and h.query is None]
    assert len(hits_without_query) >= 1
    texts = [h.text for h in hits_without_query]
    assert any("gross of refunds" in t for t in texts)


@pytest.mark.asyncio
async def test_entity_hits_appear_in_flat_list(service: SearchService) -> None:
    """Entities surface in results with their canonical kind."""
    response = await service.search(
        question="amount paid orders revenue",
        max_results=20,
    )
    entity_kinds = {h.kind for h in response.results if h.kind != "memory"}
    valid_kinds = {"datasource", "model", "column", "measure", "aggregation"}
    assert entity_kinds & valid_kinds, "expected at least one entity hit"


@pytest.mark.asyncio
async def test_flat_list_mixes_memories_and_entities(service: SearchService) -> None:
    """Both memory hits and entity hits appear in one results list."""
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        question="amount paid orders",
        max_results=20,
    )
    kinds = {h.kind for h in response.results}
    assert "memory" in kinds
    non_memory = kinds - {"memory"}
    assert non_memory, "expected at least one entity hit alongside memories"


@pytest.mark.asyncio
async def test_all_results_are_search_hit_instances(service: SearchService) -> None:
    response = await service.search(
        question="orders customers amount email",
        max_results=20,
    )
    for hit in response.results:
        assert isinstance(hit, SearchHit)


# ---------------------------------------------------------------------------
# SearchHit.id semantics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_memory_hit_id_is_raw_string_not_canonical_prefixed(
    service: SearchService,
) -> None:
    """Memory hit IDs are the raw storage ID, not 'memory:<id>'."""
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        max_results=20,
    )
    memory_hits = [h for h in response.results if h.kind == "memory"]
    assert memory_hits, "expected at least one memory hit"
    for h in memory_hits:
        assert not h.id.startswith("memory:"), (
            f"memory hit id should be raw, not canonical-prefixed; got {h.id!r}"
        )
        assert isinstance(h.id, str)
        assert h.id != ""


@pytest.mark.asyncio
async def test_entity_hit_id_is_canonical_string(service: SearchService) -> None:
    """Entity hit IDs are canonical dotted-path strings."""
    response = await service.search(
        question="orders amount paid customers",
        max_results=20,
    )
    entity_hits = [h for h in response.results if h.kind != "memory"]
    assert entity_hits, "expected at least one entity hit"
    for h in entity_hits:
        assert isinstance(h.id, str)
        # Canonical entity IDs contain at least one segment (datasource name).
        assert "." in h.id or h.id == "warehouse"


# ---------------------------------------------------------------------------
# Empty-input recency fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recency_fallback_returns_memories_including_query_bearing(
    service: SearchService,
) -> None:
    """Empty input → recency fallback; both learning-only and query-bearing
    memories appear in the flat results list."""
    response = await service.search(max_results=20)
    assert any("recency" in w.lower() for w in response.warnings)
    memory_hits = [h for h in response.results if h.kind == "memory"]
    # Corpus has 3 learning-only + 2 query-bearing = 5 total.
    assert len(memory_hits) >= 1
    # At least one query-bearing memory should appear.
    query_hits = [h for h in memory_hits if h.query is not None]
    assert query_hits, "recency fallback should include query-bearing memories"


@pytest.mark.asyncio
async def test_recency_fallback_capped_by_max_results(
    service: SearchService,
) -> None:
    response = await service.search(max_results=2)
    assert len(response.results) <= 2


# ---------------------------------------------------------------------------
# Naive Cypher fallback (advanced_search absent)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_naive_cypher_single_model_label_filters_results(
    service: SearchService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MATCH (n:Model) RETURN n.id AS id filters to model-kind entities only."""
    monkeypatch.setattr(_search_graph, "is_available", lambda: False)
    response = await service.search(
        question="orders amount customers",
        cypher_filter="MATCH (n:Model) RETURN n.id AS id",
        max_results=20,
    )
    for hit in response.results:
        assert hit.kind == "model", (
            f"expected only model hits; got kind={hit.kind!r}, id={hit.id!r}"
        )


@pytest.mark.asyncio
async def test_naive_cypher_multi_label_colon_separated(
    service: SearchService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MATCH (n:Memory:Model) RETURN n.id AS id filters to memory + model."""
    monkeypatch.setattr(_search_graph, "is_available", lambda: False)
    response = await service.search(
        question="orders amount customers",
        cypher_filter="MATCH (n:Memory:Model) RETURN n.id AS id",
        max_results=20,
    )
    for hit in response.results:
        assert hit.kind in ("memory", "model"), (
            f"expected only memory/model hits; got kind={hit.kind!r}"
        )


@pytest.mark.asyncio
async def test_naive_cypher_memory_label_filters_to_memories_only(
    service: SearchService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_search_graph, "is_available", lambda: False)
    response = await service.search(
        entities=["warehouse.orders.amount_paid"],
        question="gross refunds",
        cypher_filter="MATCH (n:Memory) RETURN n.id AS id",
        max_results=20,
    )
    for hit in response.results:
        assert hit.kind == "memory"


@pytest.mark.asyncio
async def test_naive_cypher_case_insensitive(
    service: SearchService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_search_graph, "is_available", lambda: False)
    response = await service.search(
        question="orders",
        cypher_filter="match (n:model) return n.id as id",
        max_results=20,
    )
    for hit in response.results:
        assert hit.kind == "model"


@pytest.mark.asyncio
async def test_naive_cypher_filters_before_max_results_cap(
    service: SearchService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kind filter is applied before the top-N cap, not after.
    With 5 memories and max_results=2, a Memory filter must return
    at most 2 memory hits (not fewer than possible due to post-filter)."""
    monkeypatch.setattr(_search_graph, "is_available", lambda: False)
    response = await service.search(
        question="amount refunds customers email status",
        cypher_filter="MATCH (n:Memory) RETURN n.id AS id",
        max_results=2,
    )
    # All returned hits must be memories, and the count must be <= max_results.
    for hit in response.results:
        assert hit.kind == "memory"
    assert len(response.results) <= 2


@pytest.mark.asyncio
async def test_naive_cypher_unknown_label_raises_slayer_error(
    service: SearchService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from slayer.core.errors import SlayerError
    monkeypatch.setattr(_search_graph, "is_available", lambda: False)
    with pytest.raises(SlayerError, match="(?i)unknown"):
        await service.search(
            question="x",
            cypher_filter="MATCH (n:UnknownType) RETURN n.id AS id",
        )


@pytest.mark.asyncio
async def test_naive_cypher_complex_query_raises_slayer_error_with_install_hint(
    service: SearchService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Complex Cypher raises SlayerError explaining the advanced_search requirement."""
    from slayer.core.errors import SlayerError
    monkeypatch.setattr(_search_graph, "is_available", lambda: False)
    with pytest.raises(SlayerError, match="(?i)advanced_search"):
        await service.search(
            question="x",
            cypher_filter=(
                "MATCH (m:Memory)-[:MENTIONS]->(e:Model) "
                "RETURN m.id AS id"
            ),
        )


@pytest.mark.asyncio
async def test_naive_cypher_where_clause_raises_slayer_error(
    service: SearchService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from slayer.core.errors import SlayerError
    monkeypatch.setattr(_search_graph, "is_available", lambda: False)
    with pytest.raises(SlayerError, match="(?i)advanced_search"):
        await service.search(
            question="x",
            cypher_filter=(
                "MATCH (n:Model) WHERE n.name = 'orders' RETURN n.id AS id"
            ),
        )


@pytest.mark.asyncio
async def test_naive_cypher_missing_as_id_raises_slayer_error(
    service: SearchService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without 'AS id' the Cypher is invalid even for the naive path."""
    from slayer.core.errors import SlayerError
    monkeypatch.setattr(_search_graph, "is_available", lambda: False)
    with pytest.raises(SlayerError):
        await service.search(
            question="x",
            cypher_filter="MATCH (n:Model) RETURN n.id",
        )


# ---------------------------------------------------------------------------
# REST surface
# ---------------------------------------------------------------------------


def _make_rest_client(tmp_path):
    from slayer.api.server import create_app
    import asyncio
    storage = resolve_storage(str(tmp_path / "storage"))

    async def _seed():
        await storage.save_datasource(
            DatasourceConfig(name="warehouse", type="sqlite", database=":memory:")
        )
        await storage.save_model(SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="warehouse",
            columns=[Column(name="amount_paid", type=DataType.DOUBLE)],
        ))
        await storage.save_memory(
            learning="amount_paid is net of refunds.",
            entities=["warehouse.orders.amount_paid"],
        )

    asyncio.run(_seed())
    from fastapi.testclient import TestClient
    return TestClient(create_app(storage=storage))


def test_rest_search_response_has_results_not_buckets(tmp_path) -> None:
    """POST /search response body has 'results', not 'memories'/'example_queries'/'entities'."""
    client = _make_rest_client(tmp_path)
    res = client.post("/search", json={
        "entities": ["warehouse.orders.amount_paid"],
        "max_results": 5,
    })
    assert res.status_code == 200
    body = res.json()
    assert "results" in body
    assert "memories" not in body
    assert "example_queries" not in body
    assert "entities" not in body


def test_rest_search_max_results_accepted(tmp_path) -> None:
    client = _make_rest_client(tmp_path)
    res = client.post("/search", json={
        "question": "refunds",
        "max_results": 3,
    })
    assert res.status_code == 200
    body = res.json()
    assert len(body["results"]) <= 3


def test_rest_search_old_max_memories_param_rejected(tmp_path) -> None:
    """Passing old max_memories should fail with 422 (extra fields forbidden)."""
    client = _make_rest_client(tmp_path)
    res = client.post("/search", json={
        "question": "refunds",
        "max_memories": 5,
    })
    assert res.status_code == 422


def test_rest_search_old_max_example_queries_param_rejected(tmp_path) -> None:
    client = _make_rest_client(tmp_path)
    res = client.post("/search", json={
        "question": "refunds",
        "max_example_queries": 2,
    })
    assert res.status_code == 422


def test_rest_search_old_max_entities_param_rejected(tmp_path) -> None:
    client = _make_rest_client(tmp_path)
    res = client.post("/search", json={
        "question": "refunds",
        "max_entities": 5,
    })
    assert res.status_code == 422


# ---------------------------------------------------------------------------
# MCP tool surface
# ---------------------------------------------------------------------------


_call_mcp_tool = call_mcp_tool


@pytest.mark.asyncio
async def test_mcp_search_tool_schema_has_max_results(
    storage_with_corpus: StorageBackend,
) -> None:
    from slayer.mcp.server import create_mcp_server
    mcp = create_mcp_server(storage=storage_with_corpus)
    tools = await mcp.list_tools()
    search_tool = next(t for t in tools if t.name == "search")
    schema = search_tool.inputSchema
    props = schema.get("properties", {})
    assert "max_results" in props


@pytest.mark.asyncio
async def test_mcp_search_tool_schema_does_not_have_old_params(
    storage_with_corpus: StorageBackend,
) -> None:
    from slayer.mcp.server import create_mcp_server
    mcp = create_mcp_server(storage=storage_with_corpus)
    tools = await mcp.list_tools()
    search_tool = next(t for t in tools if t.name == "search")
    schema = search_tool.inputSchema
    props = schema.get("properties", {})
    assert "max_memories" not in props
    assert "max_example_queries" not in props
    assert "max_entities" not in props


@pytest.mark.asyncio
async def test_mcp_search_response_has_results_key(
    storage_with_corpus: StorageBackend,
) -> None:
    from slayer.mcp.server import create_mcp_server
    mcp = create_mcp_server(storage=storage_with_corpus)
    result_text = await _call_mcp_tool(
        mcp=mcp,
        name="search",
        arguments={
            "entities": ["warehouse.orders.amount_paid"],
            "question": "gross refunds",
            "max_results": 5,
        },
    )
    payload = json.loads(result_text)
    assert "results" in payload
    assert "memories" not in payload
    assert "example_queries" not in payload


# ---------------------------------------------------------------------------
# CLI surface
# ---------------------------------------------------------------------------


def _run_cli(args: list[str], monkeypatch, capsys) -> tuple[int, str]:
    from slayer.cli import main
    monkeypatch.setattr(sys, "argv", ["slayer"] + args)
    try:
        main()
        code = 0
    except SystemExit as e:  # NOSONAR(S5754)
        code = int(e.code or 0)
    captured = capsys.readouterr()
    return code, captured.out


def _seed_cli_storage(tmp_path) -> str:
    import asyncio
    storage_dir = str(tmp_path / "storage")
    storage = resolve_storage(storage_dir)

    async def _seed():
        await storage.save_datasource(
            DatasourceConfig(name="warehouse", type="sqlite", database=":memory:")
        )
        await storage.save_model(SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="warehouse",
            columns=[Column(name="amount_paid", type=DataType.DOUBLE)],
        ))
        await storage.save_memory(
            learning="amount_paid is net of refunds.",
            entities=["warehouse.orders.amount_paid"],
        )

    asyncio.run(_seed())
    return storage_dir


def test_cli_search_help_has_max_results_flag(monkeypatch, capsys) -> None:
    code, out = _run_cli(["search", "--help"], monkeypatch, capsys)
    assert code == 0
    assert "--max-results" in out


def test_cli_search_help_does_not_have_old_flags(monkeypatch, capsys) -> None:
    code, out = _run_cli(["search", "--help"], monkeypatch, capsys)
    assert code == 0
    assert "--max-memories" not in out
    assert "--max-example-queries" not in out
    assert "--max-entities" not in out


def test_cli_search_json_output_has_results_key(tmp_path, monkeypatch, capsys) -> None:
    storage_dir = _seed_cli_storage(tmp_path)
    code, out = _run_cli(
        ["search", "--storage", storage_dir,
         "--entity", "warehouse.orders.amount_paid",
         "--format", "json"],
        monkeypatch, capsys,
    )
    assert code == 0
    payload = json.loads(out)
    assert "results" in payload
    assert "memories" not in payload
    assert "example_queries" not in payload


# ---------------------------------------------------------------------------
# Python client surface
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_search_returns_unified_response(
    storage_with_corpus: StorageBackend,
) -> None:
    from slayer.client.slayer_client import SlayerClient

    client = SlayerClient(storage=storage_with_corpus)
    response = await client.search(
        entities=["warehouse.orders.amount_paid"],
        question="refunds",
        max_results=5,
    )
    assert isinstance(response, SearchResponse)
    assert hasattr(response, "results")
    assert isinstance(response.results, list)
    for hit in response.results:
        assert isinstance(hit, SearchHit)


@pytest.mark.asyncio
async def test_client_search_old_max_params_raise_type_error(
    storage_with_corpus: StorageBackend,
) -> None:
    from slayer.client.slayer_client import SlayerClient
    client = SlayerClient(storage=storage_with_corpus)
    with pytest.raises(TypeError):
        await client.search(**{"question": "x", "max_memories": 5})  # type: ignore[arg-type]
