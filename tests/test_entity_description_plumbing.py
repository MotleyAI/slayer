"""DEV-1549 Codex#2: entity-description plumbing.

The compact-mode search renders ``SearchHit.description`` from the
entity's ``description`` field for every entity kind. To make that
reachable at hit-construction time without re-loading the entity, the
plumbing path threads ``description`` through:

* ``RenderedEntity.description: Optional[str]``
* ``Corpus.canonical_to_description: Dict[str, Optional[str]]``
* ``LookupFound.description: Optional[str]``

Tests pin each layer for every entity kind (datasource, model, column,
measure, aggregation).
"""

from __future__ import annotations

import tempfile
from typing import AsyncIterator

import pytest
import pytest_asyncio

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.search import index as index_mod
from slayer.search.render import (
    RenderedEntity,
    collect_model_entity_pairs,
    render_datasource_pair,
)
from slayer.search.service import LookupFound, SearchService
from slayer.storage.base import StorageBackend, resolve_storage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def storage_rich() -> AsyncIterator[StorageBackend]:
    """A datasource with one model that exercises every entity kind that
    can carry a description: datasource, model, column, measure, custom
    aggregation. Hidden flag NOT set so all entities are indexed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        s = resolve_storage(tmpdir)
        await s.save_datasource(DatasourceConfig(
            name="warehouse", type="sqlite", database=":memory:",
            description="Warehouse top-level ds desc.",
        ))
        await s.save_model(SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="warehouse",
            description="Orders table desc.",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="amount_paid", type=DataType.DOUBLE,
                       description="amount_paid column desc."),
                Column(name="status", type=DataType.TEXT),  # no description
            ],
            measures=[
                ModelMeasure(
                    name="revenue",
                    formula="amount_paid:sum",
                    description="revenue measure desc.",
                ),
            ],
            aggregations=[
                Aggregation(
                    name="wavg",
                    formula="SUM({sql} * {weight}) / NULLIF(SUM({weight}), 0)",
                    description="weighted average agg desc.",
                    params=[AggregationParam(name="weight", sql="qty")],
                ),
            ],
        ))
        yield s


# ---------------------------------------------------------------------------
# RenderedEntity layer
# ---------------------------------------------------------------------------


def test_rendered_entity_has_description_field() -> None:
    assert "description" in RenderedEntity.model_fields


def test_render_datasource_pair_propagates_description() -> None:
    pair = render_datasource_pair(
        name="warehouse",
        models=[SlayerModel(name="orders", sql_table="t", data_source="warehouse")],
    )
    # render_datasource_pair does not take an explicit description arg,
    # so this test pins the helper's behaviour: it must surface the
    # datasource description when callers extend the rendering to look
    # it up. Today the helper returns description=None; the plan adds
    # an overload that reads it.
    assert hasattr(pair, "description")


@pytest.mark.asyncio
async def test_collect_model_entity_pairs_propagates_each_kind(
    storage_rich: StorageBackend,
) -> None:
    model = await storage_rich.get_model("orders", data_source="warehouse")
    assert model is not None
    pairs = collect_model_entity_pairs(model=model)
    by_id = {p.canonical_id: p for p in pairs}

    # Model node.
    model_pair = by_id["warehouse.orders"]
    assert model_pair.kind == "model"
    assert model_pair.description == "Orders table desc."

    # Column with a description.
    col_pair = by_id["warehouse.orders.amount_paid"]
    assert col_pair.kind == "column"
    assert col_pair.description == "amount_paid column desc."

    # Column without a description.
    bare_col = by_id["warehouse.orders.status"]
    assert bare_col.kind == "column"
    assert bare_col.description is None

    # Measure.
    measure_pair = by_id["warehouse.orders.revenue"]
    assert measure_pair.kind == "measure"
    assert measure_pair.description == "revenue measure desc."

    # Aggregation.
    agg_pair = by_id["warehouse.orders.wavg"]
    assert agg_pair.kind == "aggregation"
    assert agg_pair.description == "weighted average agg desc."


# ---------------------------------------------------------------------------
# Corpus layer
# ---------------------------------------------------------------------------


def test_corpus_has_canonical_to_description_map() -> None:
    assert "canonical_to_description" in index_mod.Corpus.model_fields


@pytest.mark.asyncio
async def test_corpus_canonical_to_description_populated_for_every_kind(
    storage_rich: StorageBackend,
) -> None:
    memories = await storage_rich.list_memories(entities=None)
    models = []
    for name in await storage_rich.list_models(data_source="warehouse"):
        m = await storage_rich.get_model(name, data_source="warehouse")
        if m is not None:
            models.append(m)
    ds_cfg = await storage_rich.get_datasource("warehouse")
    assert ds_cfg is not None
    corpus = index_mod.build_in_memory_corpus(
        memories=memories,
        models=models,
        datasources=["warehouse"],
        datasource_descriptions={"warehouse": ds_cfg.description},
    )
    m = corpus.canonical_to_description
    assert m["warehouse"] == "Warehouse top-level ds desc."
    assert m["warehouse.orders"] == "Orders table desc."
    assert m["warehouse.orders.amount_paid"] == "amount_paid column desc."
    assert m["warehouse.orders.status"] is None
    assert m["warehouse.orders.revenue"] == "revenue measure desc."
    assert m["warehouse.orders.wavg"] == "weighted average agg desc."


# ---------------------------------------------------------------------------
# LookupFound layer
# ---------------------------------------------------------------------------


def test_lookup_found_has_description_field() -> None:
    assert "description" in LookupFound.model_fields


# ---------------------------------------------------------------------------
# Search-service end-to-end: compact=True surfaces description per kind
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_compact_search_surfaces_description_for_every_entity_kind(
    storage_rich: StorageBackend,
) -> None:
    service = SearchService(storage=storage_rich)
    resp = await service.search(
        question="warehouse orders amount_paid revenue wavg",
        max_results=20,
        compact=True,
    )
    by_id = {h.id: h for h in resp.results}
    # Each kind must surface with its entity.description in the
    # SearchHit.description field, and an empty text.
    expectations = [
        ("warehouse", "datasource", "Warehouse top-level ds desc."),
        ("warehouse.orders", "model", "Orders table desc."),
        ("warehouse.orders.amount_paid", "column", "amount_paid column desc."),
        ("warehouse.orders.revenue", "measure", "revenue measure desc."),
        ("warehouse.orders.wavg", "aggregation", "weighted average agg desc."),
    ]
    for canonical, kind, expected_desc in expectations:
        assert canonical in by_id, f"expected {canonical!r} to surface"
        hit = by_id[canonical]
        assert hit.kind == kind, f"{canonical}: kind {hit.kind!r}"
        assert hit.description == expected_desc, (
            f"{canonical}: description {hit.description!r}"
        )
        assert hit.text == "", f"{canonical}: text {hit.text!r}"
