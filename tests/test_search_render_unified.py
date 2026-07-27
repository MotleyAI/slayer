"""DEV-1513: unified entity-pair dispatch helpers in ``slayer.search.render``.

The plan extracts two helpers from the duplicated per-call-site dispatch
logic so the corpus build, the embedding refresh, and the new
named-entity surfacing all walk the same shape:

* ``collect_model_entity_pairs(model)`` — returns
  ``List[RenderedEntity]`` for the model + non-hidden columns + named
  measures + aggregations. Hidden models return ``[]``.
* ``render_datasource_pair(*, name, models)`` — returns one
  ``RenderedEntity`` for the datasource doc, with the visibility filter
  on ``models`` applied internally.

``RenderedEntity`` is a Pydantic ``BaseModel`` carrying
``canonical_id: str``, ``kind: str``, ``text: str``.

These tests pin the unification: the new helpers exist, all three call
sites consume them, and the embedding refresh produces byte-identical
canonical_id / entity_kind / content_hash sequences to the pre-refactor
output.
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
from slayer.embeddings import client as embedding_client
from slayer.search.retrievers.embeddings import EmbeddingRetriever
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _model_with_full_subtree() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="warehouse",
        description="Checkout orders.",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(
                name="internal_token",
                type=DataType.TEXT,
                hidden=True,
            ),
        ],
        measures=[
            ModelMeasure(name="aov", formula="amount:sum / *:count"),
        ],
        aggregations=[
            Aggregation(name="paid_only_sum", formula="SUM({col})"),
        ],
    )


def _hidden_model() -> SlayerModel:
    return SlayerModel(
        name="internal_audit",
        sql_table="public.audit",
        data_source="warehouse",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        hidden=True,
    )


@pytest.fixture
async def storage() -> AsyncIterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmp:
        s = YAMLStorage(base_dir=tmp)
        await s.save_datasource(
            DatasourceConfig(name="warehouse", type="sqlite", database=":memory:"),
        )
        await s.save_datasource(
            DatasourceConfig(name="empty_ds", type="sqlite", database=":memory:"),
        )
        await s.save_model(_model_with_full_subtree())
        await s.save_model(_hidden_model())
        yield s


# ---------------------------------------------------------------------------
# Test 28: collect_model_entity_pairs exists and produces the canonical set
# ---------------------------------------------------------------------------


def test_collect_model_entity_pairs_visible_model_produces_full_subtree() -> None:
    from slayer.search.render import collect_model_entity_pairs

    model = _model_with_full_subtree()
    pairs = collect_model_entity_pairs(model=model)
    canonicals = [p.canonical_id for p in pairs]
    kinds = {p.canonical_id: p.kind for p in pairs}

    # Model itself, every visible (non-hidden) column, every named
    # measure, every aggregation. Hidden column + unnamed measure
    # excluded.
    assert "warehouse.orders" in canonicals
    assert kinds["warehouse.orders"] == "model"
    assert "warehouse.orders.id" in canonicals
    assert kinds["warehouse.orders.id"] == "column"
    assert "warehouse.orders.amount" in canonicals
    assert "warehouse.orders.internal_token" not in canonicals
    assert "warehouse.orders.aov" in canonicals
    assert kinds["warehouse.orders.aov"] == "measure"
    assert "warehouse.orders.paid_only_sum" in canonicals
    assert kinds["warehouse.orders.paid_only_sum"] == "aggregation"


def test_collect_model_entity_pairs_hidden_model_returns_empty() -> None:
    from slayer.search.render import collect_model_entity_pairs

    pairs = collect_model_entity_pairs(model=_hidden_model())
    assert pairs == []


def test_collect_render_pairs_uses_unified_helper() -> None:
    """The corpus-build path (``index._collect_render_pairs``) consumes
    the same render output as ``collect_model_entity_pairs``. Concretely:
    the (canonical_id, kind, text) triples produced for a model in the
    corpus build are identical to what ``collect_model_entity_pairs``
    returns for that model."""
    from slayer.search.index import _collect_render_pairs
    from slayer.search.render import collect_model_entity_pairs

    model = _model_with_full_subtree()
    corpus_pairs = _collect_render_pairs(
        memories=[], visible_models=[model], datasources=[],
    )
    # Filter out the (datasource doc, etc.) — keep only the model's subtree.
    model_canonicals = {
        re.canonical_id for re in collect_model_entity_pairs(model=model)
    }
    # DEV-1549: _collect_render_pairs now yields 4-tuples
    # (canonical, kind, text, description). Strip description for parity.
    corpus_subset = [
        (c, k, t) for c, k, t, _d in corpus_pairs if c in model_canonicals
    ]
    unified = [
        (re.canonical_id, re.kind, re.text)
        for re in collect_model_entity_pairs(model=model)
    ]
    assert sorted(corpus_subset) == sorted(unified)


# ---------------------------------------------------------------------------
# Test 29: embedding refresh uses unified helper (parity vs pre-refactor)
# ---------------------------------------------------------------------------


@pytest.fixture
def stub_available(monkeypatch: pytest.MonkeyPatch) -> None:
    """Opt in to the embedding code path."""
    embedding_client._reset_query_cache()
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)


async def test_embedding_refresh_model_subtree_uses_unified_helper(
    storage: YAMLStorage,
    monkeypatch: pytest.MonkeyPatch,
    stub_available: None,
) -> None:
    """``EmbeddingRetriever.refresh_model_subtree`` produces (canonical_id,
    entity_kind, content_hash) triples that match what would be derived
    from ``collect_model_entity_pairs``. This pins the unification: any
    drift between the corpus-build dispatch and the embedding-refresh
    dispatch will fail."""
    from slayer.search.render import collect_model_entity_pairs

    captured: list[str] = []

    async def stub_embed_batch(  # NOSONAR(S7503) — stub matches embed_batch signature
        texts: list[str], *, model: str | None = None,
    ) -> list[list[float] | None]:
        # We only need to capture the rendered texts; the embedding
        # value itself is irrelevant for this parity assertion.
        captured.extend(texts)
        return [[0.0, 1.0]] * len(texts)

    monkeypatch.setattr(
        "slayer.search.retrievers.embeddings.embed_batch", stub_embed_batch,
    )

    model = await storage.get_model("orders", data_source="warehouse")
    assert model is not None
    svc = EmbeddingRetriever(storage=storage)
    await svc.refresh_model_subtree(model)

    # Every rendered text produced by the helper should appear in the
    # embedding batch — no extras, no missing entries.
    expected_texts = sorted(
        re.text for re in collect_model_entity_pairs(model=model)
    )
    assert sorted(captured) == expected_texts


# ---------------------------------------------------------------------------
# Test 30: datasource embedding/text parity across all paths, incl. zero-model
# ---------------------------------------------------------------------------


async def test_render_datasource_pair_used_by_index_and_embedding_paths(
    storage: YAMLStorage,
    monkeypatch: pytest.MonkeyPatch,
    stub_available: None,
) -> None:
    """``render_datasource_pair`` produces identical text to the
    embedding-side and index-side datasource doc renders, including for
    a datasource with zero models."""
    from slayer.search.index import _collect_render_pairs
    from slayer.search.render import render_datasource_pair

    # ---- "warehouse" (has models) -----------------------------------------
    orders = await storage.get_model("orders", data_source="warehouse")
    audit = await storage.get_model("internal_audit", data_source="warehouse")
    assert orders is not None and audit is not None

    pair_ws = render_datasource_pair(
        name="warehouse", models=[orders, audit],
    )
    assert pair_ws.canonical_id == "warehouse"
    assert pair_ws.kind == "datasource"

    corpus_pairs = _collect_render_pairs(
        memories=[],
        visible_models=[orders],  # hidden model already filtered upstream
        datasources=["warehouse"],
    )
    corpus_ws_text = next(
        t for c, k, t, _d in corpus_pairs if c == "warehouse"
    )
    assert pair_ws.text == corpus_ws_text

    # Embedding-side: stub embed_batch and inspect the captured text.
    captured: list[str] = []

    async def stub_embed_batch(  # NOSONAR(S7503) — stub matches embed_batch signature
        texts: list[str], *, model: str | None = None,
    ) -> list[list[float] | None]:
        captured.extend(texts)
        return [[0.0, 1.0]] * len(texts)

    monkeypatch.setattr(
        "slayer.search.retrievers.embeddings.embed_batch", stub_embed_batch,
    )
    svc = EmbeddingRetriever(storage=storage)
    await svc.refresh_datasource(name="warehouse", models=[orders, audit])
    assert pair_ws.text in captured

    # ---- "empty_ds" (zero models) -----------------------------------------
    pair_empty = render_datasource_pair(name="empty_ds", models=[])
    assert pair_empty.canonical_id == "empty_ds"
    assert pair_empty.kind == "datasource"
    # Should produce some non-empty text (datasource doc is valid w/o models).
    assert pair_empty.text != ""

    captured.clear()
    await svc.refresh_datasource(name="empty_ds", models=[])
    assert pair_empty.text in captured


# ---------------------------------------------------------------------------
# Extra regression: RenderedEntity is a Pydantic BaseModel with the expected
# field shape (no dataclass!).
# ---------------------------------------------------------------------------


def test_rendered_entity_is_pydantic_model_with_expected_fields() -> None:
    from pydantic import BaseModel

    from slayer.search.render import RenderedEntity

    assert issubclass(RenderedEntity, BaseModel)
    re = RenderedEntity(
        canonical_id="warehouse.orders", kind="model", text="…",
    )
    assert re.canonical_id == "warehouse.orders"
    assert re.kind == "model"
    assert re.text == "…"


# ---------------------------------------------------------------------------
# Sentinel-based call-path proofs: every dispatch site actually invokes
# the unified helper (parity alone could be satisfied by duplicated logic).
# ---------------------------------------------------------------------------


def test_index_corpus_path_actually_calls_collect_model_entity_pairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pinned by monkeypatching the symbol where ``_collect_render_pairs``
    looks it up: if the corpus dispatch still has a private duplicate,
    the sentinel never appears in the output."""
    from slayer.search.render import RenderedEntity
    from slayer.search import index as index_mod

    sentinel = RenderedEntity(
        canonical_id="SENTINEL.x", kind="model", text="SENTINEL_TEXT",
    )

    def stub(*, model: SlayerModel) -> list[RenderedEntity]:  # noqa: ARG001
        return [sentinel]

    monkeypatch.setattr(index_mod, "collect_model_entity_pairs", stub)
    pairs = index_mod._collect_render_pairs(
        memories=[],
        visible_models=[_model_with_full_subtree()],
        datasources=[],
    )
    # DEV-1549: 4-tuples (canonical, kind, text, description).
    assert any(
        c == "SENTINEL.x" and t == "SENTINEL_TEXT"
        for c, k, t, _d in pairs
    )


async def test_embedding_path_actually_calls_collect_model_entity_pairs(
    storage: YAMLStorage,
    monkeypatch: pytest.MonkeyPatch,
    stub_available: None,
) -> None:
    """Pinned by monkeypatching the symbol where ``refresh_model_subtree``
    looks it up: if the embedding dispatch still has a private duplicate,
    the sentinel text never reaches the embedding batch."""
    from slayer.search.render import RenderedEntity
    from slayer.search.retrievers import embeddings as emb_mod

    sentinel_text = "SENTINEL_EMB_TEXT"
    sentinel = RenderedEntity(
        canonical_id="SENTINEL.x", kind="model", text=sentinel_text,
    )

    def stub(*, model: SlayerModel) -> list[RenderedEntity]:  # noqa: ARG001
        return [sentinel]

    monkeypatch.setattr(emb_mod, "collect_model_entity_pairs", stub)

    captured: list[str] = []

    async def stub_embed_batch(  # NOSONAR(S7503) — stub matches embed_batch signature
        texts: list[str], *, model: str | None = None,  # noqa: ARG001
    ) -> list[list[float] | None]:
        captured.extend(texts)
        return [[0.0, 1.0]] * len(texts)

    monkeypatch.setattr(emb_mod, "embed_batch", stub_embed_batch)

    model = await storage.get_model("orders", data_source="warehouse")
    assert model is not None
    await EmbeddingRetriever(storage=storage).refresh_model_subtree(model)
    assert sentinel_text in captured


def test_index_corpus_path_actually_calls_render_datasource_pair(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same sentinel pattern for the datasource doc — the corpus path
    must route through ``render_datasource_pair``."""
    from slayer.search.render import RenderedEntity
    from slayer.search import index as index_mod

    sentinel = RenderedEntity(
        canonical_id="SENTINEL_DS", kind="datasource",
        text="SENTINEL_DS_TEXT",
    )

    def stub(
        *,
        name: str,
        models: list[SlayerModel],
        description: str | None = None,
    ) -> RenderedEntity:  # noqa: ARG001
        return sentinel

    monkeypatch.setattr(index_mod, "render_datasource_pair", stub)
    pairs = index_mod._collect_render_pairs(
        memories=[], visible_models=[], datasources=["anything"],
    )
    # DEV-1549: 4-tuples.
    assert any(
        c == "SENTINEL_DS" and t == "SENTINEL_DS_TEXT"
        for c, k, t, _d in pairs
    )


async def test_embedding_path_actually_calls_render_datasource_pair(
    storage: YAMLStorage,
    monkeypatch: pytest.MonkeyPatch,
    stub_available: None,
) -> None:
    """Same sentinel pattern for the embedding-side datasource refresh."""
    from slayer.search.render import RenderedEntity
    from slayer.search.retrievers import embeddings as emb_mod

    sentinel_text = "SENTINEL_DS_EMB_TEXT"
    sentinel = RenderedEntity(
        canonical_id="SENTINEL_DS", kind="datasource", text=sentinel_text,
    )

    def stub(
        *,
        name: str,
        models: list[SlayerModel],
        description: str | None = None,
    ) -> RenderedEntity:  # noqa: ARG001
        return sentinel

    monkeypatch.setattr(emb_mod, "render_datasource_pair", stub)

    captured: list[str] = []

    async def stub_embed_batch(  # NOSONAR(S7503) — stub matches embed_batch signature
        texts: list[str], *, model: str | None = None,  # noqa: ARG001
    ) -> list[list[float] | None]:
        captured.extend(texts)
        return [[0.0, 1.0]] * len(texts)

    monkeypatch.setattr(emb_mod, "embed_batch", stub_embed_batch)

    await EmbeddingRetriever(storage=storage).refresh_datasource(
        name="warehouse", models=[],
    )
    assert sentinel_text in captured
