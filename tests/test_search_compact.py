"""DEV-1549: compact-by-default search.

Tests cover:
* ``SearchHit.description`` field present.
* ``SearchService.search`` accepts ``compact: bool = True``.
* compact=True semantics for memory hits: description = Memory.description
  if set, else first-paragraph fallback (capped at 500 chars); text = "".
* compact=False semantics: description = Memory.description if set, else
  None (NO fallback); text = full learning rendering.
* No special-case for "description:" keyword in the fallback.
* Entity hits surface entity.description into SearchHit.description.
* Drill-in pattern: search(entities=["memory:<id>"], max_results=1,
  compact=False) returns the full body.
* Empty-input recency fallback honours compact.
* Compact + lazy column-sample refresh: text stays empty (Codex#3).
* Compact + DEV-1428 stale entity tags: tags are filtered out before
  matched_entities is computed (compact mode does not bypass this).
* Compact + DEV-1464 cypher_filter pre-filter.
* Compact + datasource pre-filter.
* Compact + empty-input + datasource.
* matched_entities is identical across modes.
* query field on example-query hits is preserved in both modes.
"""

from __future__ import annotations

import tempfile
from typing import AsyncIterator

import pytest
import pytest_asyncio

from tests.search_helpers import seed_warehouse_models

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.search.service import SearchHit, SearchService
from slayer.storage.base import StorageBackend, resolve_storage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = resolve_storage(tmpdir)
        await seed_warehouse_models(s)
        yield s


@pytest_asyncio.fixture
async def service(storage: StorageBackend) -> SearchService:
    return SearchService(storage=storage)


# ---------------------------------------------------------------------------
# SearchHit shape
# ---------------------------------------------------------------------------


def test_search_hit_has_description_field() -> None:
    fields = SearchHit.model_fields
    assert "description" in fields


def test_search_hit_description_defaults_none() -> None:
    hit = SearchHit(kind="memory", id="1", score=0.5, text="hi")
    assert hit.description is None


# ---------------------------------------------------------------------------
# Service signature
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_service_accepts_compact_kwarg(
    service: SearchService,
) -> None:
    resp = await service.search(question="x", compact=True)
    assert resp is not None


@pytest.mark.asyncio
async def test_search_service_compact_default_true(
    storage: StorageBackend,
) -> None:
    """Codex#7 / spec: compact defaults to True everywhere."""
    import inspect

    sig = inspect.signature(SearchService.search)
    assert sig.parameters["compact"].default is True


# ---------------------------------------------------------------------------
# Memory hit compact rendering
# ---------------------------------------------------------------------------


async def _save_memory_returning_id(
    storage: StorageBackend,
    *,
    learning: str,
    entities: list,
    description=None,
    query=None,
) -> str:
    mem = await storage.save_memory(
        learning=learning,
        entities=entities,
        description=description,
        query=query,
    )
    return mem.id


@pytest.mark.asyncio
async def test_memory_hit_compact_uses_description_when_set(
    storage: StorageBackend, service: SearchService,
) -> None:
    mem_id = await _save_memory_returning_id(
        storage,
        learning="a very long body that should not appear in compact mode",
        entities=["warehouse.orders.amount_paid"],
        description="paid revenue note",
    )
    resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=True,
    )
    hit = next(h for h in resp.results if h.kind == "memory" and h.id == mem_id)
    assert hit.description == "paid revenue note"
    assert hit.text == ""


@pytest.mark.asyncio
async def test_memory_hit_compact_falls_back_to_first_paragraph(
    storage: StorageBackend, service: SearchService,
) -> None:
    learning = "first paragraph line.\n\nsecond paragraph body."
    mem_id = await _save_memory_returning_id(
        storage,
        learning=learning,
        entities=["warehouse.orders.amount_paid"],
    )
    resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=True,
    )
    hit = next(h for h in resp.results if h.kind == "memory" and h.id == mem_id)
    assert hit.description == "first paragraph line."
    assert hit.text == ""


@pytest.mark.asyncio
async def test_compact_fallback_caps_at_500_chars(
    storage: StorageBackend, service: SearchService,
) -> None:
    long_line = "x" * 1000  # no blank line → first paragraph is whole thing
    mem_id = await _save_memory_returning_id(
        storage,
        learning=long_line,
        entities=["warehouse.orders.amount_paid"],
    )
    resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=True,
    )
    hit = next(h for h in resp.results if h.kind == "memory" and h.id == mem_id)
    assert hit.description is not None
    assert len(hit.description) == 500


@pytest.mark.asyncio
async def test_compact_fallback_single_line_learning(
    storage: StorageBackend, service: SearchService,
) -> None:
    mem_id = await _save_memory_returning_id(
        storage,
        learning="just one line",
        entities=["warehouse.orders.amount_paid"],
    )
    resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=True,
    )
    hit = next(h for h in resp.results if h.kind == "memory" and h.id == mem_id)
    assert hit.description == "just one line"


@pytest.mark.asyncio
async def test_compact_fallback_no_description_keyword_special_case(
    storage: StorageBackend, service: SearchService,
) -> None:
    """Brief proposed a `description:`/`Description:` 2nd-line special-case;
    user explicitly rejected it. The keyword line is part of the first
    paragraph with no special handling."""
    learning = "header line.\ndescription: This line is part of paragraph 1.\n\nbody."
    mem_id = await _save_memory_returning_id(
        storage,
        learning=learning,
        entities=["warehouse.orders.amount_paid"],
    )
    resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=True,
    )
    hit = next(h for h in resp.results if h.kind == "memory" and h.id == mem_id)
    # First paragraph is BOTH lines (no special stripping of "description:" prefix).
    assert hit.description == (
        "header line.\ndescription: This line is part of paragraph 1."
    )


# ---------------------------------------------------------------------------
# compact=False — no fallback, full text restored
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_false_no_fallback_when_description_absent(
    storage: StorageBackend, service: SearchService,
) -> None:
    mem_id = await _save_memory_returning_id(
        storage,
        learning="full body of the memory",
        entities=["warehouse.orders.amount_paid"],
    )
    resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=False,
    )
    hit = next(h for h in resp.results if h.kind == "memory" and h.id == mem_id)
    assert hit.description is None
    assert "full body of the memory" in hit.text


@pytest.mark.asyncio
async def test_compact_false_keeps_description_when_set(
    storage: StorageBackend, service: SearchService,
) -> None:
    mem_id = await _save_memory_returning_id(
        storage,
        learning="full body",
        entities=["warehouse.orders.amount_paid"],
        description="summary",
    )
    resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=False,
    )
    hit = next(h for h in resp.results if h.kind == "memory" and h.id == mem_id)
    assert hit.description == "summary"
    assert "full body" in hit.text


# ---------------------------------------------------------------------------
# Example-query hits — query field preserved across modes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_example_query_hit_compact_preserves_query(
    storage: StorageBackend, service: SearchService,
) -> None:
    sample = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="amount_paid:sum")],
    )
    mem_id = await _save_memory_returning_id(
        storage,
        learning="paid sum example.",
        entities=["warehouse.orders.amount_paid"],
        query=sample,
        description="paid sum demo",
    )
    resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=True,
    )
    hit = next(h for h in resp.results if h.kind == "memory" and h.id == mem_id)
    assert hit.query is not None
    assert hit.description == "paid sum demo"
    assert hit.text == ""


@pytest.mark.asyncio
async def test_example_query_hit_verbose_preserves_query(
    storage: StorageBackend, service: SearchService,
) -> None:
    sample = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="amount_paid:sum")],
    )
    mem_id = await _save_memory_returning_id(
        storage,
        learning="paid sum body.",
        entities=["warehouse.orders.amount_paid"],
        query=sample,
    )
    resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=False,
    )
    hit = next(h for h in resp.results if h.kind == "memory" and h.id == mem_id)
    assert hit.query is not None
    assert "paid sum body" in hit.text


# ---------------------------------------------------------------------------
# Entity hits — description from the entity model
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_entity_hit_compact_surfaces_entity_description(
    service: SearchService,
) -> None:
    """The seeded `amount_paid` column has description="Net paid in USD."."""
    resp = await service.search(question="amount paid net usd", compact=True)
    column_hits = [
        h for h in resp.results
        if h.kind == "column" and h.id == "warehouse.orders.amount_paid"
    ]
    assert column_hits, "expected the amount_paid column to surface"
    hit = column_hits[0]
    assert hit.description == "Net paid in USD."
    assert hit.text == ""


@pytest.mark.asyncio
async def test_entity_hit_compact_description_none_when_entity_has_none(
    storage: StorageBackend, service: SearchService,
) -> None:
    """A column without a description gets SearchHit.description = None."""
    await storage.save_model(SlayerModel(
        name="bare", sql_table="bare", data_source="warehouse",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="raw_amount", type=DataType.DOUBLE),  # no description
        ],
    ))
    resp = await service.search(question="raw_amount", compact=True)
    column_hits = [
        h for h in resp.results
        if h.kind == "column" and h.id == "warehouse.bare.raw_amount"
    ]
    assert column_hits
    hit = column_hits[0]
    assert hit.description is None
    assert hit.text == ""


@pytest.mark.asyncio
async def test_entity_hit_verbose_uses_full_render_text(
    service: SearchService,
) -> None:
    resp = await service.search(question="amount paid net usd", compact=False)
    column_hits = [
        h for h in resp.results
        if h.kind == "column" and h.id == "warehouse.orders.amount_paid"
    ]
    assert column_hits
    hit = column_hits[0]
    # Verbose text is the full multi-line render — first line names the column.
    assert "warehouse.orders.amount_paid" in hit.text
    assert hit.description == "Net paid in USD."


# ---------------------------------------------------------------------------
# Drill-in pattern
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drill_in_returns_full_body(
    storage: StorageBackend, service: SearchService,
) -> None:
    mem_id = await _save_memory_returning_id(
        storage,
        learning="a full body the agent wants to read after drilling in",
        entities=["warehouse.orders.amount_paid"],
        description="short summary",
    )
    resp = await service.search(
        entities=[f"memory:{mem_id}"], max_results=1, compact=False,
    )
    hit = next(h for h in resp.results if h.id == mem_id)
    assert "full body" in hit.text
    assert hit.description == "short summary"


# ---------------------------------------------------------------------------
# Empty-input fallback honours compact
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_input_fallback_honours_compact(
    storage: StorageBackend, service: SearchService,
) -> None:
    await _save_memory_returning_id(
        storage,
        learning="recency body",
        entities=["warehouse.orders.amount_paid"],
        description="recency preview",
    )
    resp = await service.search(compact=True)  # no entities/query/question
    mem_hits = [h for h in resp.results if h.kind == "memory"]
    assert mem_hits
    for h in mem_hits:
        assert h.text == ""
        assert h.description is not None


# ---------------------------------------------------------------------------
# matched_entities unchanged by compact
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_matched_entities_unchanged_by_compact(
    storage: StorageBackend, service: SearchService,
) -> None:
    mem_id = await _save_memory_returning_id(
        storage,
        learning="x",
        entities=["warehouse.orders.amount_paid", "warehouse.orders.status"],
    )
    compact_resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=True,
    )
    verbose_resp = await service.search(
        entities=["warehouse.orders.amount_paid"], compact=False,
    )
    compact_hit = next(
        h for h in compact_resp.results if h.kind == "memory" and h.id == mem_id
    )
    verbose_hit = next(
        h for h in verbose_resp.results if h.kind == "memory" and h.id == mem_id
    )
    assert compact_hit.matched_entities == verbose_hit.matched_entities


# ---------------------------------------------------------------------------
# Codex#3: compact + lazy column-sample refresh
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_plus_lazy_column_refresh_keeps_text_empty(
    storage: StorageBackend, monkeypatch,
) -> None:
    """Codex#3 (strengthened): force the lazy column-refresh path to
    actually run (monkeypatch ensure_column_sample_fresh to return a
    materially-updated column) and assert compact=True keeps text="" while
    description still reflects the freshly refreshed column."""
    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.search import service as svc_mod

    # A real engine satisfies the truthy-engine gate in SearchService.
    engine = SlayerQueryEngine(storage=storage)
    service = SearchService(storage=storage, engine=engine)

    refreshed_calls = []

    async def fake_refresh(*, model, column, engine, storage):  # NOSONAR(S7503) — async required to match the monkeypatched call site (`await ensure_column_sample_fresh(...)`)
        # Return a materially-different column so the production path
        # treats it as "refreshed" (the identity check is ``is col``).
        refreshed_calls.append((model.name, column.name))
        return column.model_copy(update={"description": "FRESH"})

    monkeypatch.setattr(svc_mod, "ensure_column_sample_fresh", fake_refresh)

    resp = await service.search(question="amount paid net", compact=True)
    column_hits = [h for h in resp.results if h.kind == "column"]
    assert column_hits, "expected at least one column hit to trigger refresh"
    assert refreshed_calls, "expected the refresh hook to fire"
    for h in column_hits:
        assert h.text == ""
    # The refreshed description must surface on at least one column hit.
    assert any(h.description == "FRESH" for h in column_hits)


@pytest.mark.asyncio
async def test_verbose_plus_lazy_column_refresh_regenerates_text(
    storage: StorageBackend, monkeypatch,
) -> None:
    """Verbose-mode regression pin: refresh DOES regenerate hit.text."""
    from slayer.engine.query_engine import SlayerQueryEngine
    from slayer.search import service as svc_mod

    engine = SlayerQueryEngine(storage=storage)
    service = SearchService(storage=storage, engine=engine)

    async def fake_refresh(*, model, column, engine, storage):  # NOSONAR(S7503) — async required to match the monkeypatched call site (`await ensure_column_sample_fresh(...)`)
        return column.model_copy(update={"description": "FRESH"})

    monkeypatch.setattr(svc_mod, "ensure_column_sample_fresh", fake_refresh)

    resp = await service.search(question="amount paid", compact=False)
    column_hits = [h for h in resp.results if h.kind == "column"]
    assert column_hits
    # Verbose text is the full render — first line names the column.
    assert any(
        "warehouse.orders" in h.text or "amount_paid" in h.text
        for h in column_hits
    )


# ---------------------------------------------------------------------------
# Codex#10–12: compact × stale tags / cypher_filter / datasource pre-filter
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_with_stale_entity_tags(
    storage: StorageBackend, service: SearchService,
) -> None:
    """DEV-1428 stale-tag filtering must apply BEFORE matched_entities is
    surfaced, regardless of compact mode.

    Setup: save memory with a stale tag, then call search with the stale
    tag itself as input. Without DEV-1428 filtering the stale tag would
    appear in matched_entities. Compact mode must not bypass the filter.
    """
    mem_id = await _save_memory_returning_id(
        storage,
        learning="orders body",
        entities=[
            "warehouse.orders.amount_paid",
            "warehouse.orders.does_not_exist_anymore",
        ],
        description="ok",
    )
    resp = await service.search(
        entities=[
            "warehouse.orders.amount_paid",
            "warehouse.orders.does_not_exist_anymore",
        ],
        compact=True,
    )
    hits = [h for h in resp.results if h.kind == "memory" and h.id == mem_id]
    assert hits, "expected memory to surface via the live tag"
    hit = hits[0]
    # The stale tag must NOT appear in matched_entities even though the
    # user passed it as input — the DEV-1428 filter strips it server-side.
    assert "warehouse.orders.does_not_exist_anymore" not in hit.matched_entities
    assert "warehouse.orders.amount_paid" in hit.matched_entities
    assert hit.text == ""
    assert hit.description == "ok"


@pytest.mark.asyncio
async def test_compact_with_cypher_filter_excludes_non_matching(
    storage: StorageBackend, service: SearchService,
) -> None:
    """DEV-1464 cypher_filter allowlist must apply to compact-mode hits.

    Setup: save a memory that WOULD surface without the filter
    (baseline), then re-search with the column-kind cypher filter and
    assert (a) the memory is excluded, (b) at least one column hit
    survives, and (c) the surviving column hit is rendered in compact
    shape (text="", description set)."""
    mem_id = await _save_memory_returning_id(
        storage,
        learning="filtered out by cypher_filter",
        entities=["warehouse.orders.amount_paid"],
        description="memory desc",
    )
    # Baseline: memory IS reachable without the filter.
    baseline = await service.search(question="amount paid", compact=True)
    assert any(h.id == mem_id for h in baseline.results)

    # The full-graph engine uses label "ModelColumn"; the naive-fallback
    # parser uses "Column". Both should narrow to column-only hits.
    from slayer.search import graph as _graph_mod
    column_label = "ModelColumn" if _graph_mod.is_available() else "Column"
    filtered = await service.search(
        question="amount paid",
        cypher_filter=f"MATCH (n:{column_label}) RETURN n.id AS id",
        compact=True,
    )
    assert all(h.id != mem_id for h in filtered.results)
    column_hits = [h for h in filtered.results if h.kind == "column"]
    assert column_hits, "expected column hits to survive the cypher filter"
    for h in column_hits:
        assert h.text == ""


@pytest.mark.asyncio
async def test_compact_with_datasource_pre_filter(
    storage: StorageBackend, service: SearchService,
) -> None:
    """`datasource=` arg limits the corpus across all channels.

    Baseline first: without the filter, the other_ds column surfaces.
    Then assert the filter excludes other_ds AND warehouse hits survive
    in compact shape.
    """
    await storage.save_datasource(
        DatasourceConfig(name="other_ds", type="sqlite", database=":memory:")
    )
    await storage.save_model(SlayerModel(
        name="other_table", sql_table="t", data_source="other_ds",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount_paid", type=DataType.DOUBLE,
                   description="other ds amount_paid"),
        ],
    ))
    baseline = await service.search(question="amount paid", compact=True)
    assert any(h.id.startswith("other_ds") for h in baseline.results), \
        "baseline must surface other_ds hits"

    resp = await service.search(
        question="amount paid", datasource="warehouse", compact=True,
    )
    assert all(not h.id.startswith("other_ds") for h in resp.results)
    warehouse_column_hits = [
        h for h in resp.results
        if h.kind == "column" and h.id.startswith("warehouse.")
    ]
    assert warehouse_column_hits, "warehouse hits must survive the filter"
    for h in warehouse_column_hits:
        assert h.text == ""


@pytest.mark.asyncio
async def test_compact_empty_input_with_datasource(
    storage: StorageBackend, service: SearchService,
) -> None:
    """Empty-input recency fallback + datasource + compact returns
    scoped memories in compact shape."""
    mem_id = await _save_memory_returning_id(
        storage,
        learning="warehouse memory body",
        entities=["warehouse.orders.amount_paid"],
        description="wh preview",
    )
    resp = await service.search(datasource="warehouse", compact=True)
    hit = next(h for h in resp.results if h.kind == "memory" and h.id == mem_id)
    assert hit.description == "wh preview"
    assert hit.text == ""
