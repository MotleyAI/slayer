"""DEV-1513: ``search(entities=[...])`` surfaces named entities themselves.

When the user names a canonical ref via ``entities=`` (or via
``query=``), channel 1's BM25 over entity-tag overlap is extended via
implicit self-references:

* Every entity is conceptually tagged with itself, so a named entity
  surfaces in the ``entities`` bucket.
* Every memory's effective tag list is augmented with
  ``memory:<self_id>``, so a ``memory:<id>`` ref in ``entities=``
  surfaces the named memory in the matching bucket (``memories`` or
  ``example_queries``).

Filters: refs not rooted at ``datasource`` and refs on hidden
models/columns drop with warnings. ``max_entities=0`` continues to
suppress all entity output. Per-bucket invariance (DEV-1414) preserved.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.search.service import SearchService
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Fixture: corpus with two datasources, hidden entities, measures, aggs
# ---------------------------------------------------------------------------


def _make_corpus_models() -> list[SlayerModel]:
    return [
        SlayerModel(
            name="orders",
            sql_table="public.orders",
            data_source="warehouse",
            description="Checkout orders.",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(
                    name="amount",
                    type=DataType.DOUBLE,
                    description="Net amount in cents.",
                ),
                Column(
                    name="status",
                    type=DataType.TEXT,
                    description="paid|refunded.",
                ),
                Column(
                    name="internal_token",
                    type=DataType.TEXT,
                    description="Internal payment token.",
                    hidden=True,
                ),
            ],
            measures=[
                ModelMeasure(
                    name="aov",
                    formula="amount:sum / *:count",
                    description="Average order value.",
                ),
            ],
            aggregations=[
                Aggregation(
                    name="paid_only_sum",
                    formula="SUM(CASE WHEN status='paid' THEN {col} END)",
                    description="Sum gated by paid status.",
                ),
            ],
        ),
        SlayerModel(
            name="customers",
            sql_table="public.customers",
            data_source="warehouse",
            description="Customer master.",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="email", type=DataType.TEXT),
            ],
        ),
        SlayerModel(
            name="internal_audit",
            sql_table="public.audit",
            data_source="warehouse",
            description="Internal audit log.",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="event", type=DataType.TEXT),
            ],
            hidden=True,
        ),
        SlayerModel(
            name="products",
            sql_table="public.products",
            data_source="other_db",
            description="Product catalog (other ds).",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
            ],
        ),
    ]


@pytest.fixture
async def storage() -> AsyncIterator[YAMLStorage]:
    """Seeded YAMLStorage with two datasources (``warehouse`` and
    ``other_db``), several models including a hidden model and a hidden
    column, plus a third datasource ``empty_ds`` that owns no models."""
    with tempfile.TemporaryDirectory() as tmp:
        s = YAMLStorage(base_dir=tmp)
        await s.save_datasource(
            DatasourceConfig(name="warehouse", type="sqlite", database=":memory:"),
        )
        await s.save_datasource(
            DatasourceConfig(name="other_db", type="sqlite", database=":memory:"),
        )
        await s.save_datasource(
            DatasourceConfig(name="empty_ds", type="sqlite", database=":memory:"),
        )
        for m in _make_corpus_models():
            await s.save_model(m)
        yield s


@pytest.fixture
async def service(storage: YAMLStorage) -> SearchService:
    return SearchService(storage=storage)


# ---------------------------------------------------------------------------
# Tests 1–5: named entity of each kind surfaces in entities bucket
# ---------------------------------------------------------------------------


async def test_named_column_surfaces_in_entities_bucket(
    service: SearchService,
) -> None:
    # DEV-1549: opt out of compact-by-default to exercise the verbose
    # render text contract this test was written for.
    resp = await service.search(
        entities=["warehouse.orders.amount"],
        max_results=20,
        compact=False,
    )
    memory_hits = [h for h in resp.results if h.kind == "memory"]
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert memory_hits == []
    assert len(entity_hits) == 1
    hit = entity_hits[0]
    assert hit.id == "warehouse.orders.amount"
    assert hit.kind == "column"
    assert "amount" in hit.text


async def test_named_model_surfaces_in_entities_bucket(
    service: SearchService,
) -> None:
    resp = await service.search(
        entities=["warehouse.orders"],
        max_results=20,
        compact=False,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert len(entity_hits) == 1
    hit = entity_hits[0]
    assert hit.id == "warehouse.orders"
    assert hit.kind == "model"
    assert "orders" in hit.text


async def test_named_datasource_surfaces_in_entities_bucket(
    service: SearchService,
) -> None:
    resp = await service.search(
        entities=["warehouse"],
        max_results=20,
        compact=False,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert len(entity_hits) == 1
    hit = entity_hits[0]
    assert hit.id == "warehouse"
    assert hit.kind == "datasource"
    assert "warehouse" in hit.text


async def test_named_measure_surfaces_in_entities_bucket(
    service: SearchService,
) -> None:
    resp = await service.search(
        entities=["warehouse.orders.aov"],
        max_results=20,
        compact=False,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert len(entity_hits) == 1
    hit = entity_hits[0]
    assert hit.id == "warehouse.orders.aov"
    assert hit.kind == "measure"
    assert "aov" in hit.text


async def test_named_aggregation_surfaces_in_entities_bucket(
    service: SearchService,
) -> None:
    resp = await service.search(
        entities=["warehouse.orders.paid_only_sum"],
        max_results=20,
        compact=False,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert len(entity_hits) == 1
    hit = entity_hits[0]
    assert hit.id == "warehouse.orders.paid_only_sum"
    assert hit.kind == "aggregation"
    assert "paid_only_sum" in hit.text


# ---------------------------------------------------------------------------
# Test 6: named + fuzzy combine in entities bucket
# ---------------------------------------------------------------------------


async def test_named_and_fuzzy_combine_in_entities_bucket(
    service: SearchService,
) -> None:
    """Channel 1 named + channels 2/3 fuzzy both contribute to entities."""
    resp = await service.search(
        entities=["warehouse.orders.amount"],
        question="customer email",
        max_results=20,
    )
    ids = [h.id for h in resp.results if h.kind != "memory"]
    # Named ref always present.
    assert "warehouse.orders.amount" in ids
    # Fuzzy match for "customer email" surfaces customers / its email column.
    assert any(i in ids for i in (
        "warehouse.customers", "warehouse.customers.email",
    ))


# ---------------------------------------------------------------------------
# Test 7: unknown ref becomes warning, not entity hit
# ---------------------------------------------------------------------------


async def test_unknown_named_ref_is_warning_not_entity_hit(
    service: SearchService,
) -> None:
    resp = await service.search(
        entities=["warehouse.orders.no_such_column"],
        max_results=20,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert entity_hits == []
    assert any("no_such_column" in w for w in resp.warnings)


# ---------------------------------------------------------------------------
# Test 8: off-datasource ref drops with warning
# ---------------------------------------------------------------------------


async def test_off_datasource_named_ref_drops_with_warning(
    service: SearchService,
) -> None:
    resp = await service.search(
        entities=["other_db.products"],
        datasource="warehouse",
        max_results=20,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert entity_hits == []
    assert any(
        "other_db.products" in w
        and "not rooted at datasource 'warehouse'" in w
        for w in resp.warnings
    )


# ---------------------------------------------------------------------------
# Test 9: hidden model drops with warning
# ---------------------------------------------------------------------------


async def test_hidden_model_named_ref_drops_with_warning(
    service: SearchService,
) -> None:
    resp = await service.search(
        entities=["warehouse.internal_audit"],
        max_results=20,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert entity_hits == []
    assert any(
        "warehouse.internal_audit" in w
        and "hidden" in w
        for w in resp.warnings
    )


# ---------------------------------------------------------------------------
# Test 10: hidden column drops with warning
# ---------------------------------------------------------------------------


async def test_hidden_column_named_ref_drops_with_warning(
    service: SearchService,
) -> None:
    resp = await service.search(
        entities=["warehouse.orders.internal_token"],
        max_results=20,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert entity_hits == []
    assert any(
        "warehouse.orders.internal_token" in w
        and "hidden" in w
        for w in resp.warnings
    )


# ---------------------------------------------------------------------------
# Tests 11–13: memory:<id> in entities surfaces the named memory
# ---------------------------------------------------------------------------


async def test_memory_id_in_entities_surfaces_memory_in_memories_bucket(
    storage: YAMLStorage, service: SearchService,
) -> None:
    seed = await storage.save_memory(
        learning="net revenue equals amount minus refunds",
        entities=["warehouse.orders.amount"],
    )
    resp = await service.search(
        entities=[f"memory:{seed.id}"],
        max_results=20,
    )
    memory_hits = [h for h in resp.results if h.kind == "memory" and h.query is None]
    example_query_hits = [h for h in resp.results if h.kind == "memory" and h.query is not None]
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    ids = [h.id for h in memory_hits]
    assert seed.id in ids
    assert example_query_hits == []
    assert entity_hits == []


async def test_memory_id_in_entities_surfaces_query_bearing_in_example_queries(
    storage: YAMLStorage, service: SearchService,
) -> None:
    seed = await storage.save_memory(
        learning="example: revenue rollup pattern",
        entities=["warehouse.orders.amount"],
        query=SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
        ),
    )
    resp = await service.search(
        entities=[f"memory:{seed.id}"],
        max_results=20,
    )
    example_query_hits = [h for h in resp.results if h.kind == "memory" and h.query is not None]
    memory_hits = [h for h in resp.results if h.kind == "memory" and h.query is None]
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    eq_ids = [h.id for h in example_query_hits]
    assert seed.id in eq_ids
    assert memory_hits == []
    assert entity_hits == []


async def test_memory_id_off_datasource_drops_with_warning(
    storage: YAMLStorage, service: SearchService,
) -> None:
    seed = await storage.save_memory(
        learning="catalog note about products",
        entities=["other_db.products"],
    )
    resp = await service.search(
        entities=[f"memory:{seed.id}"],
        datasource="warehouse",
        max_results=20,
    )
    memory_hits = [h for h in resp.results if h.kind == "memory"]
    assert memory_hits == []
    assert any(
        f"memory:{seed.id}" in w
        and "not rooted at datasource 'warehouse'" in w
        for w in resp.warnings
    )


# ---------------------------------------------------------------------------
# Test 14: named entity surfaces when max_results is large enough
# ---------------------------------------------------------------------------


async def test_named_entity_surfaces_when_max_results_large(
    service: SearchService,
) -> None:
    """Under DEV-1532's flat results API, a user-supplied canonical
    entity (with no matching memory) surfaces as an entity hit as soon
    as ``max_results`` has room. Replaces the legacy
    ``max_entities=0`` suppression test — that knob no longer exists."""
    resp = await service.search(
        entities=["warehouse.orders.amount"],
        max_results=20,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert any(h.id == "warehouse.orders.amount" for h in entity_hits)


# ---------------------------------------------------------------------------
# Test 15: query= arg surfaces extracted entities
# ---------------------------------------------------------------------------


async def test_query_arg_entities_also_surface_in_entities_bucket(
    service: SearchService,
) -> None:
    q = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="amount:sum")],
    )
    resp = await service.search(
        query=q,
        max_results=20,
    )
    ids = [h.id for h in resp.results if h.kind != "memory"]
    # The query references orders and orders.amount; both should surface.
    assert "warehouse.orders" in ids
    assert "warehouse.orders.amount" in ids


# ---------------------------------------------------------------------------
# Test 16: named ref at top of entities bucket when no fuzzy overlap
# ---------------------------------------------------------------------------


async def test_named_ref_at_top_of_entities_when_no_fuzzy_overlap(
    service: SearchService,
) -> None:
    """A named ref with no fuzzy contribution sits at the top of the
    entities bucket (channel 1 contributes rank 1; fuzzy channels rank
    other docs lower)."""
    resp = await service.search(
        entities=["warehouse.orders.status"],
        question="completely unrelated phrase",
        max_results=20,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert entity_hits, "expected at least one entity hit"
    assert entity_hits[0].id == "warehouse.orders.status"


# ---------------------------------------------------------------------------
# Test 17: RRF fuses when named + fuzzy reference same canonical
# ---------------------------------------------------------------------------


async def test_rrf_fuses_named_and_fuzzy_on_same_canonical(
    service: SearchService,
) -> None:
    """The same canonical contributed by both channel 1 (named) and
    channel 2 (fuzzy) appears once, with strictly higher RRF score than
    either channel alone (proves both channels contribute)."""
    resp_named_only = await service.search(
        entities=["warehouse.orders.amount"],
        max_results=20,
    )
    resp_fuzzy_only = await service.search(
        question="amount",
        max_results=20,
    )
    resp_combined = await service.search(
        entities=["warehouse.orders.amount"],
        question="amount",
        max_results=20,
    )
    # Sanity: the fuzzy channel alone surfaces the same canonical so the
    # "combined" call really has two channels contributing the same ref.
    assert "warehouse.orders.amount" in [
        h.id for h in resp_fuzzy_only.results if h.kind != "memory"
    ]
    named_only_score = next(
        h.score for h in resp_named_only.results
        if h.kind != "memory" and h.id == "warehouse.orders.amount"
    )
    fuzzy_only_score = next(
        h.score for h in resp_fuzzy_only.results
        if h.kind != "memory" and h.id == "warehouse.orders.amount"
    )
    combined_score = next(
        h.score for h in resp_combined.results
        if h.kind != "memory" and h.id == "warehouse.orders.amount"
    )
    assert combined_score > named_only_score
    assert combined_score > fuzzy_only_score
    # No duplicate.
    entity_ids = [h.id for h in resp_combined.results if h.kind != "memory"]
    assert entity_ids.count("warehouse.orders.amount") == 1


# ---------------------------------------------------------------------------
# Test 18: matched_entities includes memory:<self_id> when user named it
# ---------------------------------------------------------------------------


async def test_matched_entities_includes_memory_self_ref_when_named(
    storage: YAMLStorage, service: SearchService,
) -> None:
    seed = await storage.save_memory(
        learning="refund window is 30 days",
        entities=["warehouse.orders.amount"],
    )
    resp = await service.search(
        entities=[f"memory:{seed.id}"],
        max_results=20,
    )
    memory_hits = [h for h in resp.results if h.kind == "memory" and h.query is None]
    hit = next(h for h in memory_hits if h.id == seed.id)
    assert f"memory:{seed.id}" in hit.matched_entities


# ---------------------------------------------------------------------------
# Test 22: mixed entities and memory: refs land in different buckets
# ---------------------------------------------------------------------------


async def test_mixed_memory_id_and_entity_refs_split_across_buckets(
    storage: YAMLStorage, service: SearchService,
) -> None:
    seed = await storage.save_memory(
        learning="amount is in cents",
        entities=["warehouse.orders.amount"],
    )
    resp = await service.search(
        entities=[f"memory:{seed.id}", "warehouse.orders.status"],
        max_results=20,
    )
    memory_hits = [h for h in resp.results if h.kind == "memory"]
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    assert seed.id in [h.id for h in memory_hits]
    assert "warehouse.orders.status" in [h.id for h in entity_hits]
    # The memory ref does NOT leak into the entities bucket.
    assert f"memory:{seed.id}" not in [h.id for h in entity_hits]
    # The entity ref does NOT leak into the memories bucket.
    assert "warehouse.orders.status" not in [h.id for h in memory_hits]


# ---------------------------------------------------------------------------
# Test 23: pure-named datasource text includes visible models, excludes hidden
# ---------------------------------------------------------------------------


async def test_pure_named_datasource_text_includes_visible_excludes_hidden(
    service: SearchService,
) -> None:
    """A bare-datasource named hit's text references visible models and
    omits hidden ones (parity with the corpus build's
    ``render_datasource_text`` visibility filter)."""
    resp = await service.search(
        entities=["warehouse"],
        max_results=20,
        compact=False,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    hit = next(h for h in entity_hits if h.id == "warehouse")
    assert "orders" in hit.text
    assert "customers" in hit.text
    assert "internal_audit" not in hit.text


# ---------------------------------------------------------------------------
# Test 24: named hit with question active reuses corpus text/kind
# ---------------------------------------------------------------------------


async def test_named_hit_with_question_active_reuses_corpus_path(
    service: SearchService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a corpus is built (``question`` is active), the named hit
    reads (kind, text) from ``corpus.canonical_to_*`` rather than re-
    rendering directly. Pinned by monkeypatching the direct-render
    helper in ``slayer.search.service`` to raise: if the corpus path is
    used, the helper is never called and the search succeeds."""
    def fail_if_called(*args, **kwargs):  # noqa: ANN001 — test-only sentinel
        raise AssertionError(
            "direct-render fallback called when corpus already covers the canonical"
        )
    monkeypatch.setattr(
        "slayer.search.service.collect_model_entity_pairs",
        fail_if_called,
    )
    resp = await service.search(
        entities=["warehouse.orders.amount"],
        question="amount in cents",
        max_results=20,
        compact=False,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    hit = next(
        h for h in entity_hits if h.id == "warehouse.orders.amount"
    )
    assert hit.kind == "column"
    assert "amount" in hit.text


# ---------------------------------------------------------------------------
# Tests 25–26: hidden named ref drops from entity bucket but memories
# tagged with that canonical still surface
# ---------------------------------------------------------------------------


async def test_hidden_model_drops_entity_but_memories_still_surface(
    storage: YAMLStorage, service: SearchService,
) -> None:
    seed = await storage.save_memory(
        learning="internal audit retention is 7 years",
        entities=["warehouse.internal_audit"],
    )
    resp = await service.search(
        entities=["warehouse.internal_audit"],
        max_results=20,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    memory_hits = [h for h in resp.results if h.kind == "memory" and h.query is None]
    # Entity bucket: dropped with warning.
    assert entity_hits == []
    assert any("internal_audit" in w and "hidden" in w for w in resp.warnings)
    # Memory bucket: still surfaces — BM25 over original tags unaffected.
    assert seed.id in [h.id for h in memory_hits]


async def test_hidden_column_drops_entity_but_memories_still_surface(
    storage: YAMLStorage, service: SearchService,
) -> None:
    seed = await storage.save_memory(
        learning="internal_token rotates monthly",
        entities=["warehouse.orders.internal_token"],
    )
    resp = await service.search(
        entities=["warehouse.orders.internal_token"],
        max_results=20,
    )
    entity_hits = [h for h in resp.results if h.kind != "memory"]
    memory_hits = [h for h in resp.results if h.kind == "memory" and h.query is None]
    assert entity_hits == []
    assert any(
        "internal_token" in w and "hidden" in w
        for w in resp.warnings
    )
    assert seed.id in [h.id for h in memory_hits]


# ---------------------------------------------------------------------------
# Test 27: stale-query warning fires for explicitly-named memory:<id>
# regardless of max_example_queries cap
# ---------------------------------------------------------------------------


async def _make_amount_stale(storage: YAMLStorage) -> None:
    """Re-save ``warehouse.orders`` without the ``amount`` column so any
    memory tagged or query referencing ``amount`` becomes stale."""
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="warehouse",
        description="Checkout orders.",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
        ],
    ))


async def test_stale_query_warning_for_named_memory_id_default_cap(
    storage: YAMLStorage, service: SearchService,
) -> None:
    seed = await storage.save_memory(
        learning="legacy revenue lookup",
        entities=["warehouse.orders.amount"],
        query=SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
        ),
    )
    # Make `amount` stale — the attached query no longer resolves.
    await _make_amount_stale(storage)
    resp = await service.search(
        entities=[f"memory:{seed.id}"],
        max_results=20,
    )
    example_query_hits = [h for h in resp.results if h.kind == "memory" and h.query is not None]
    # Hit is surfaced and a stale-query warning fires.
    assert seed.id in [h.id for h in example_query_hits]
    assert any(
        f"memory:{seed.id}" in w and "stale" in w
        for w in resp.warnings
    )


async def test_stale_query_warning_for_named_memory_id_under_tight_cap(
    storage: YAMLStorage, service: SearchService,
) -> None:
    """Under a tight ``max_results=1`` cap, an explicitly-named
    ``memory:<id>`` pointing at a query-bearing memory with stale refs
    still emits the stale-query warning even when the cap may suppress
    the hit itself."""
    seed = await storage.save_memory(
        learning="legacy revenue lookup",
        entities=["warehouse.orders.amount"],
        query=SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
        ),
    )
    await _make_amount_stale(storage)
    resp = await service.search(
        entities=[f"memory:{seed.id}"],
        max_results=1,
    )
    assert any(
        f"memory:{seed.id}" in w and "stale" in w
        for w in resp.warnings
    )


# ---------------------------------------------------------------------------
# Test 31: self-ref preserved when all stored entity tags are stale
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Missing-after-resolution: a canonical resolves but the model/datasource
# is gone by the time _lookup_named_entity runs. Returns Missing (not
# Hidden) and the caller emits a missing-style warning, distinct from
# the hidden-on-model warning.
# ---------------------------------------------------------------------------


async def test_lookup_returns_missing_when_model_deleted_after_resolution(
    storage: YAMLStorage,
) -> None:
    """``_lookup_named_entity`` returns ``Missing`` (not ``Hidden``) when
    the canonical resolved successfully but the model was deleted in the
    window between resolution and lookup."""
    from slayer.search.service import (
        _lookup_named_entity,
        LookupFound,
        LookupHidden,
        LookupMissing,
    )

    canonical = "warehouse.orders.amount"
    # Sanity: it exists pre-delete.
    result = await _lookup_named_entity(
        canonical=canonical, storage=storage, corpus=None,
    )
    assert isinstance(result, LookupFound)

    # Delete the parent model. Resolution is not re-run; we simulate the
    # race by deleting between resolve and lookup at the function level.
    await storage.delete_model("orders", data_source="warehouse")
    result = await _lookup_named_entity(
        canonical=canonical, storage=storage, corpus=None,
    )
    assert isinstance(result, LookupMissing)
    assert not isinstance(result, LookupHidden)


async def test_lookup_returns_missing_when_bare_datasource_deleted(
    storage: YAMLStorage,
) -> None:
    """Bare ``<ds>`` lookup re-verifies datasource membership via
    ``list_datasources()`` — a datasource that's gone returns ``Missing``,
    not a stale render."""
    from slayer.search.service import (
        _lookup_named_entity,
        LookupFound,
        LookupMissing,
    )

    canonical = "empty_ds"
    pre = await _lookup_named_entity(
        canonical=canonical, storage=storage, corpus=None,
    )
    assert isinstance(pre, LookupFound)

    await storage.delete_datasource("empty_ds")
    post = await _lookup_named_entity(
        canonical=canonical, storage=storage, corpus=None,
    )
    assert isinstance(post, LookupMissing)


# ---------------------------------------------------------------------------
# Test 31 (kept here in canonical order)
# ---------------------------------------------------------------------------


async def test_memory_self_ref_preserved_when_all_stored_tags_are_stale(
    storage: YAMLStorage, service: SearchService,
) -> None:
    """Edge case: a memory whose stored ``entities`` are ALL stale
    canonicals (every tag dropped by ``_filter_memories_entities``).
    The synthetic ``memory:<self_id>`` must still survive so the named
    ``memory:<id>`` ref surfaces the memory."""
    seed = await storage.save_memory(
        learning="archived note about the deleted_col column",
        entities=["warehouse.orders.amount"],
    )
    # Make the only tag stale by re-saving the model without the column.
    await _make_amount_stale(storage)
    resp = await service.search(
        entities=[f"memory:{seed.id}"],
        max_results=20,
    )
    memory_hits = [h for h in resp.results if h.kind == "memory" and h.query is None]
    assert seed.id in [h.id for h in memory_hits]
