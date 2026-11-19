"""A pre-rename embeddings sidecar's row kinds become ``dataset`` / ``field`` / ``metric`` in place, in both
backends and across reopens; only a row whose text changed (a field naming its datatype) is re-embedded.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.

Spec: compatibility/ossie-vocabulary › Stored documents migrate to the new vocabulary (Stored embedding kinds are
renamed in place).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from slayer.embeddings import client as embedding_client
from slayer.embeddings.models import Embedding
from slayer.search.retrievers.embeddings import EmbeddingRetriever
from slayer.storage.base import StorageBackend
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._deprecated_input_helpers import recorded
from tests._ossie_vocabulary_fixtures import shop_customers, shop_orders, shop_store
from tests._stored_upgrade_fixtures import BACKENDS, DS

EMBEDDER = "test/embedder"
CREATED_AT = "2026-01-01T00:00:00+00:00"
NEW_KIND = {"model": "dataset", "column": "field", "measure": "metric"}

#: (canonical_id, pre-rename kind, content hash) as the pre-rename refresh wrote them for the ``shop`` datasets.
PRE_RENAME_ROWS: list[tuple[str, str, str]] = [
    ("shop.customers", "model", "3bd05e457f58cdfe5593c095c3714f8666b0efdc267f5611416c8d0f037c4039"),
    ("shop.customers.id", "column", "0d9aea4de748ecf701c84aa094699e1472c4fb2a91f11d012d9902e6bb56025b"),
    ("shop.customers.name", "column", "8fd5b48e3da444e987fb5ffb04b93f2a699cd5bdc971b40198d9a594743fd3cd"),
    ("shop.customers.region", "column", "de7cf7b214f9ca8c5d05a0736f34b3311e714fc24b0fb7e9e5f69830c30870cb"),
    ("shop.customers.signed_up_at", "column", "05323643dbf5791516aea294789ff4907335f0fc006494f7dcda1fc00204c813"),
    ("shop.orders", "model", "42645e7c10674fac5390c4c0c80f31dde7c0447f8740d409368bfdacfa02833d"),
    ("shop.orders.amount", "column", "2604e69e3de1d308d93f2b60ad0a50a37abc61f446d41c0ec3eb87b3f10ff71e"),
    ("shop.orders.customer_id", "column", "5f5574228d4aba6c50671145ba5df2ea3a12d57bc2d05a4c5218fd116a9c942e"),
    ("shop.orders.order_id", "column", "6d28343f333915790911bdfe327a2c72ce00f01a43c213ea0ce95572a0f93cd3"),
    ("shop.orders.ordered_at", "column", "78d39c1b0aabe349bc9df501c9811d12459a6bafb41f2b5b43788f4a785f4bdb"),
    ("shop.orders.revenue", "measure", "4c7f956789e0993f72523b421ec398b98258fa127a1b4f45031b03587ee2427d"),
    ("shop.orders.status", "column", "870695660688c76e93872793cc38d370bc3e94236afeb27247ab720847c65500"),
]


def sidecar_path(*, backend: str, base: Path) -> str:
    return str(base / "embeddings.db") if backend == "yaml" else str(base / "store.db")


def reopen(*, backend: str, base: Path) -> StorageBackend:
    return YAMLStorage(base_dir=str(base)) if backend == "yaml" else SQLiteStorage(db_path=str(base / "store.db"))


def raw_rows(*, backend: str, base: Path) -> dict[str, tuple[Any, ...]]:
    """canonical_id -> (kind, content hash, vector, created_at), read verbatim."""
    with transaction(sidecar_path(backend=backend, base=base)) as conn:
        rows = conn.execute(
            "SELECT canonical_id, entity_kind, content_hash, embedding, created_at FROM embeddings "
            "WHERE embedding_model_name = ?", (EMBEDDER,),
        ).fetchall()
    return {r[0]: tuple(r[1:]) for r in rows}


async def pre_rename_store(*, backend: str, base: Path) -> None:
    """A store holding the ``shop`` datasets at their pre-rename version and the pre-rename sidecar rows."""
    await shop_store(backend=backend, base=str(base), datasets=[
        {"version": 25, **shop_customers("old")}, {"version": 25, **shop_orders("old")},
    ])
    with transaction(sidecar_path(backend=backend, base=base)) as conn:
        conn.executemany(
            "INSERT INTO embeddings (canonical_id, embedding_model_name, entity_kind, content_hash, embedding, "
            "created_at) VALUES (?, ?, ?, ?, ?, ?)",
            [(cid, EMBEDDER, kind, digest, json.dumps([1.0, 0.5, 0.25]), CREATED_AT)
             for cid, kind, digest in PRE_RENAME_ROWS],
        )


class _CountingEmbed:
    def __init__(self) -> None:
        self.texts: list[str] = []

    async def __call__(self, texts: list[str], *, model: str | None = None) -> list[list[float] | None]:
        self.texts.extend(texts)
        return [[0.0, 0.0, 0.0] for _ in texts]


@pytest.fixture
def counting_embed(monkeypatch: pytest.MonkeyPatch) -> _CountingEmbed:
    embed = _CountingEmbed()
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    monkeypatch.setattr("slayer.search.retrievers.embeddings.embed_batch", embed)
    return embed


def expected_rows() -> dict[str, tuple[Any, ...]]:
    return {cid: (NEW_KIND[kind], digest, json.dumps([1.0, 0.5, 0.25]), CREATED_AT)
            for cid, kind, digest in PRE_RENAME_ROWS}


@pytest.mark.parametrize("backend", BACKENDS)
class TestSidecarKinds:
    async def test_opening_renames_every_kind_in_place(self, backend: str, tmp_path: Path) -> None:
        await pre_rename_store(backend=backend, base=tmp_path)
        reopen(backend=backend, base=tmp_path)
        assert raw_rows(backend=backend, base=tmp_path) == expected_rows()

    async def test_the_rename_survives_a_restart(self, backend: str, tmp_path: Path) -> None:
        await pre_rename_store(backend=backend, base=tmp_path)
        reopen(backend=backend, base=tmp_path)
        first = raw_rows(backend=backend, base=tmp_path)
        storage = reopen(backend=backend, base=tmp_path)
        assert raw_rows(backend=backend, base=tmp_path) == first == expected_rows()
        listed = await storage.list_embeddings(embedding_model_name=EMBEDDER)
        assert sorted((r.canonical_id, r.entity_kind) for r in listed) == sorted(
            (cid, NEW_KIND[kind]) for cid, kind, _ in PRE_RENAME_ROWS)

    async def test_only_rows_whose_text_changed_are_re_embedded(
        self, backend: str, tmp_path: Path, counting_embed: _CountingEmbed,
    ) -> None:
        await pre_rename_store(backend=backend, base=tmp_path)
        storage = reopen(backend=backend, base=tmp_path)
        retriever = EmbeddingRetriever(storage=storage, model_name=EMBEDDER)
        for name in ("customers", "orders"):
            dataset = await storage.get_dataset(name, data_source=DS)
            assert dataset is not None
            assert await retriever.refresh_dataset_subtree(dataset) == []
        fields = {cid for cid, kind, _ in PRE_RENAME_ROWS if kind == "column"}
        assert sorted(t.split("\n", 1)[0].removeprefix("Column: ") for t in counting_embed.texts) == sorted(fields)
        assert all("\nType: " in t for t in counting_embed.texts)
        rows = raw_rows(backend=backend, base=tmp_path)
        assert {cid: row for cid, row in rows.items() if cid not in fields} == {
            cid: row for cid, row in expected_rows().items() if cid not in fields}
        assert all(rows[cid][0] == "field" and rows[cid][1] != expected_rows()[cid][1] for cid in fields)


class TestEmbeddingDocument:
    @pytest.mark.parametrize(("old", "new"), [
        *NEW_KIND.items(), ("memory", "memory"), ("datasource", "datasource"), ("aggregation", "aggregation"),
    ])
    def test_a_version_1_row_steps_silently(self, old: str, new: str) -> None:
        with recorded() as caught:
            row = Embedding.model_validate({
                "version": 1, "canonical_id": "shop.orders", "embedding_model_name": EMBEDDER,
                "entity_kind": old, "content_hash": "h", "embedding": [0.1],
            })
        assert [w for w in caught if issubclass(w.category, FutureWarning)] == []
        assert (row.entity_kind, row.version) == (new, 2)

    def test_the_current_version_is_2(self) -> None:
        assert CURRENT_VERSIONS["Embedding"] == 2
