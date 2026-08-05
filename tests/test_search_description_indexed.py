"""DEV-1549 (post-merge): `Memory.description` is indexed into the
lexical render so BM25 / Tantivy can match terms that live only in the
description field. Without this, installs without the optional
embedding extra would lose recall for the new field.
"""

from __future__ import annotations

import tempfile
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from tests.search_helpers import seed_warehouse_models

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.memories.models import Memory
from slayer.search.render import render_datasource_text, render_memory_text
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend, resolve_storage


def test_render_memory_text_includes_description() -> None:
    mem = Memory(
        learning="amount in cents",
        description="precision-critical column for billing",
    )
    rendered = render_memory_text(memory=mem)
    assert "amount in cents" in rendered
    assert "precision-critical column for billing" in rendered


def test_render_memory_text_omits_none_description() -> None:
    mem = Memory(learning="amount in cents", description=None)
    rendered = render_memory_text(memory=mem)
    assert rendered.startswith("amount in cents")
    assert "None" not in rendered


@pytest_asyncio.fixture
async def storage() -> AsyncIterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        s = resolve_storage(tmpdir)
        await seed_warehouse_models(s)
        yield s


@pytest.mark.asyncio
async def test_tantivy_matches_description_only_term(
    storage: StorageBackend,
) -> None:
    """A `question=` search whose terms appear ONLY in the description
    field surfaces the memory via the tantivy channel. Pinning the lexical
    index parity with the embedding index."""
    await storage.save_memory(
        learning="orders body unrelated terms",
        entities=["warehouse.orders.amount_paid"],
        description="reconciliation_token_xyz keyword that lives only in description",
    )
    response = await SearchService(storage=storage).search(
        question="reconciliation_token_xyz",
        max_results=10,
    )
    memory_hits = [h for h in response.results if h.kind == "memory"]
    assert memory_hits, (
        "expected the description-only term to surface via tantivy"
    )


# ---------------------------------------------------------------------------
# Datasource description — DEV-1549 round 3
# ---------------------------------------------------------------------------


def test_render_datasource_text_includes_description() -> None:
    rendered = render_datasource_text(
        name="warehouse",
        models=[],
        description="prod warehouse with billing facts",
    )
    assert "warehouse" in rendered
    assert "prod warehouse with billing facts" in rendered


def test_render_datasource_text_omits_none_description() -> None:
    rendered = render_datasource_text(name="warehouse", models=[])
    assert "warehouse" in rendered
    assert "Description:" not in rendered


@pytest.mark.asyncio
async def test_tantivy_matches_datasource_description_only_term() -> None:
    """A `question=` search whose terms appear ONLY in the datasource's
    description surfaces the datasource entity via tantivy. Pinning the
    parity gap codex flagged on round 3."""
    with tempfile.TemporaryDirectory() as tmpdir:
        s = resolve_storage(tmpdir)
        await s.save_datasource(DatasourceConfig(
            name="warehouse",
            type="sqlite",
            database=":memory:",
            description="amalgamated_reconciliation_zeta is a unique term",
        ))
        await s.save_model(SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="warehouse",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
            ],
        ))
        response = await SearchService(storage=s).search(
            question="amalgamated_reconciliation_zeta",
            max_results=10,
        )
    ds_hits = [
        h for h in response.results
        if h.kind == "datasource" and h.id == "warehouse"
    ]
    assert ds_hits, (
        "expected datasource-description-only term to surface via tantivy"
    )
