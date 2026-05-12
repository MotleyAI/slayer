"""Storage-layer tests for the Embedding sidecar table (DEV-1386).

Parameterised across the two real storage backends (YAML + SQLite) so
every contract — upsert, fetch, list, prefix cascade — is verified
identically against both. The backend factory yields a fresh
``base_dir``/``db_path`` per test.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from typing import Iterator, List

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
from slayer.embeddings.models import Embedding
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture(params=["yaml", "sqlite"])
def storage(request: pytest.FixtureRequest) -> Iterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmp:
        if request.param == "yaml":
            yield YAMLStorage(base_dir=tmp)
        else:
            yield SQLiteStorage(db_path=os.path.join(tmp, "test.db"))


def _embed(
    *,
    canonical_id: str,
    model: str = "openai/test-embedding",
    kind: str = "memory",
    text_hash: str = "h0",
    vector: List[float] | None = None,
) -> Embedding:
    return Embedding(
        canonical_id=canonical_id,
        embedding_model_name=model,
        entity_kind=kind,
        content_hash=text_hash,
        embedding=vector or [0.1, 0.2, 0.3],
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


async def test_round_trip_save_and_get(storage: StorageBackend) -> None:
    row = _embed(canonical_id="memory:1")
    await storage.save_embedding(row)
    fetched = await storage.get_embedding(
        canonical_id="memory:1",
        embedding_model_name="openai/test-embedding",
    )
    assert fetched is not None
    assert fetched.canonical_id == "memory:1"
    assert fetched.embedding == [0.1, 0.2, 0.3]
    assert fetched.entity_kind == "memory"
    assert fetched.content_hash == "h0"


async def test_get_unknown_returns_none(storage: StorageBackend) -> None:
    assert await storage.get_embedding(
        canonical_id="missing",
        embedding_model_name="openai/test-embedding",
    ) is None


async def test_save_is_idempotent_upsert(storage: StorageBackend) -> None:
    """Re-saving the same (canonical_id, model) replaces the row in place."""
    first = _embed(canonical_id="ds.m.col", kind="column", text_hash="h-a")
    second = _embed(
        canonical_id="ds.m.col",
        kind="column",
        text_hash="h-b",
        vector=[9.9, 9.9, 9.9],
    )
    await storage.save_embedding(first)
    await storage.save_embedding(second)
    fetched = await storage.get_embedding(
        canonical_id="ds.m.col",
        embedding_model_name="openai/test-embedding",
    )
    assert fetched is not None
    assert fetched.content_hash == "h-b"
    assert fetched.embedding == [9.9, 9.9, 9.9]
    # Only one row exists.
    rows = await storage.list_embeddings(
        embedding_model_name="openai/test-embedding",
    )
    assert len(rows) == 1


async def test_list_filters_by_model_name(storage: StorageBackend) -> None:
    """list_embeddings only returns rows for the requested model."""
    await storage.save_embedding(_embed(
        canonical_id="memory:1", model="openai/a",
    ))
    await storage.save_embedding(_embed(
        canonical_id="memory:1", model="openai/b",
    ))
    await storage.save_embedding(_embed(
        canonical_id="memory:2", model="openai/a",
    ))
    a_rows = await storage.list_embeddings(embedding_model_name="openai/a")
    b_rows = await storage.list_embeddings(embedding_model_name="openai/b")
    assert {r.canonical_id for r in a_rows} == {"memory:1", "memory:2"}
    assert {r.canonical_id for r in b_rows} == {"memory:1"}


async def test_delete_for_canonical_prefix_cascades(
    storage: StorageBackend,
) -> None:
    """delete_embeddings_for_canonical wipes every row whose
    canonical_id starts with the supplied prefix — used by the
    delete_model / delete_datasource cascade."""
    rows_to_seed = [
        _embed(canonical_id="ds_a.orders", kind="model"),
        _embed(canonical_id="ds_a.orders.id", kind="column"),
        _embed(canonical_id="ds_a.orders.revenue", kind="column"),
        _embed(canonical_id="ds_a.customers", kind="model"),
        _embed(canonical_id="ds_b.orders", kind="model"),
    ]
    for row in rows_to_seed:
        await storage.save_embedding(row)

    removed = await storage.delete_embeddings_for_canonical(
        canonical_id_prefix="ds_a.orders",
    )
    assert removed == 3
    remaining = await storage.list_embeddings(
        embedding_model_name="openai/test-embedding",
    )
    assert {r.canonical_id for r in remaining} == {
        "ds_a.customers", "ds_b.orders",
    }


async def test_delete_no_match_returns_zero(storage: StorageBackend) -> None:
    await storage.save_embedding(_embed(canonical_id="memory:1"))
    removed = await storage.delete_embeddings_for_canonical(
        canonical_id_prefix="no.match",
    )
    assert removed == 0
    assert len(await storage.list_embeddings(
        embedding_model_name="openai/test-embedding",
    )) == 1


async def test_delete_memory_cascades_embedding(
    storage: StorageBackend,
) -> None:
    """``delete_memory`` on the ABC wrapper must drop the matching
    embedding row keyed by ``memory:<id>``."""
    memory = await storage.save_memory(
        learning="alpha", entities=["e1"], query=None,
    )
    await storage.save_embedding(_embed(
        canonical_id=f"memory:{memory.id}", kind="memory",
    ))
    assert await storage.get_embedding(
        canonical_id=f"memory:{memory.id}",
        embedding_model_name="openai/test-embedding",
    ) is not None
    await storage.delete_memory(memory.id)
    assert await storage.get_embedding(
        canonical_id=f"memory:{memory.id}",
        embedding_model_name="openai/test-embedding",
    ) is None


async def test_delete_model_cascades_subtree_embeddings(
    storage: StorageBackend,
) -> None:
    """``delete_model`` on the ABC wrapper must drop the model doc
    embedding plus every column / measure / aggregation embedding
    keyed by the model's canonical prefix."""
    # Seed a model so the ABC delete wrapper can resolve it.
    ds = DatasourceConfig(
        name="dsx", type="postgres", host="h", database="d",
    )
    await storage.save_datasource(ds)
    model = SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="dsx",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        measures=[ModelMeasure(name="rev", formula="id:sum")],
        aggregations=[Aggregation(name="custom_agg", formula="SUM({x})")],
    )
    await storage.save_model(model)

    for cid, kind in [
        ("dsx.orders", "model"),
        ("dsx.orders.id", "column"),
        ("dsx.orders.rev", "measure"),
        ("dsx.orders.custom_agg", "aggregation"),
        ("dsx.customers", "model"),
        ("other.orders", "model"),
    ]:
        await storage.save_embedding(_embed(canonical_id=cid, kind=kind))

    await storage.delete_model("orders", data_source="dsx")

    remaining = {
        r.canonical_id for r in await storage.list_embeddings(
            embedding_model_name="openai/test-embedding",
        )
    }
    assert remaining == {"dsx.customers", "other.orders"}


async def test_delete_datasource_cascades_descendants(
    storage: StorageBackend,
) -> None:
    """``delete_datasource`` cascades every embedding under the
    datasource prefix — datasource doc + every model / column /
    measure / aggregation under it."""
    ds = DatasourceConfig(
        name="dsx", type="postgres", host="h", database="d",
    )
    await storage.save_datasource(ds)
    for cid, kind in [
        ("dsx", "datasource"),
        ("dsx.orders", "model"),
        ("dsx.orders.id", "column"),
        ("other", "datasource"),
        ("other.orders", "model"),
    ]:
        await storage.save_embedding(_embed(canonical_id=cid, kind=kind))

    await storage.delete_datasource("dsx")

    remaining = {
        r.canonical_id for r in await storage.list_embeddings(
            embedding_model_name="openai/test-embedding",
        )
    }
    assert remaining == {"other", "other.orders"}
