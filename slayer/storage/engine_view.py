"""A query engine's non-owning view of its store: loads that need the live datasource read it through the engine."""

from __future__ import annotations

from collections.abc import Hashable
from typing import Any

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.embeddings.models import Embedding
from slayer.memories.models import Memory
from slayer.storage.base import ClientSource, StorageBackend
from slayer.storage.document_loading import Loaded


class EngineStorage(StorageBackend):
    """``inner`` as one query engine reads it; closing the view never closes ``inner``."""

    def __init__(self, inner: StorageBackend, *, client_source: ClientSource) -> None:
        # Another engine's view is unwrapped: a view always reads its own engine's clients.
        self._inner = inner.inner if isinstance(inner, EngineStorage) else inner
        self._client_source = client_source
        self._ids_collide_as_filenames = getattr(self._inner, "_ids_collide_as_filenames", False)

    @property
    def inner(self) -> StorageBackend:
        return self._inner

    def __getattr__(self, name: str) -> Any:
        # Backend-specific attributes (paths, handles) read through to the store.
        if name == "_inner":
            raise AttributeError(name)
        return getattr(self._inner, name)

    async def aclose(self) -> None:
        """No-op: the store outlives every engine viewing it."""

    # ---- models -------------------------------------------------------------

    async def get_model(self, name: str, data_source: str | None = None) -> SlayerModel | None:
        return await self._inner.get_model(name, data_source=data_source)

    async def _list_all_model_identities(self) -> list[tuple[str, str]]:
        return await self._inner._list_all_model_identities()

    async def _load_raw_model_dict(self, *, name: str, data_source: str) -> dict | None:
        return await self._inner._load_raw_model_dict(name=name, data_source=data_source)

    async def _save_model_impl(self, model: SlayerModel) -> None:
        await self._inner._save_model_impl(model)

    async def _delete_model_row(self, *, data_source: str, name: str) -> bool:
        return await self._inner._delete_model_row(data_source=data_source, name=name)

    async def update_column_sampled(
        self, *, data_source: str, model_name: str, column_name: str,
        sampled: str | None, sampled_values: list[str] | None, distinct_count: int | None,
    ) -> None:
        await self._inner.update_column_sampled(
            data_source=data_source, model_name=model_name, column_name=column_name,
            sampled=sampled, sampled_values=sampled_values, distinct_count=distinct_count,
        )

    async def graph_fingerprint(self) -> str | None:
        return await self._inner.graph_fingerprint()

    async def cache_identity(self) -> Hashable:
        return await self._inner.cache_identity()

    # ---- datasources --------------------------------------------------------

    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        await self._inner._save_datasource_impl(datasource)

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
        return await self._inner.get_datasource(name)

    async def list_datasources(self) -> list[str]:
        return await self._inner.list_datasources()

    async def _delete_datasource_row(self, name: str) -> bool:
        return await self._inner._delete_datasource_row(name)

    async def get_datasource_priority(self) -> list[str]:
        return await self._inner.get_datasource_priority()

    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        await self._inner._set_datasource_priority_raw(priority)

    # ---- memories -----------------------------------------------------------

    async def _save_memory_row(self, memory: Memory) -> None:
        await self._inner._save_memory_row(memory)

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        return await self._inner._get_memory_row(memory_id)

    async def _list_memories_rows(self, *, entities: list[str] | None) -> Loaded[Memory]:
        return await self._inner._list_memories_rows(entities=entities)

    async def _delete_memory_row(self, memory_id: str) -> bool:
        return await self._inner._delete_memory_row(memory_id)

    async def _next_memory_seq(self) -> str:
        return await self._inner._next_memory_seq()

    async def _memory_ids(self) -> list[str]:
        return await self._inner._memory_ids()

    async def save_memory(
        self,
        *,
        learning: str,
        entities: list[str],
        query: SlayerQuery | None = None,
        id: str | None = None,  # noqa: A002 — mirrors the base signature
        description: str | None = None,
    ) -> Memory:
        return await self._inner.save_memory(
            learning=learning, entities=entities, query=query, id=id, description=description,
        )

    async def strip_dangling_entities_from_memories(self, *, canonical_id: str) -> int:
        return await self._inner.strip_dangling_entities_from_memories(canonical_id=canonical_id)

    # ---- embeddings ---------------------------------------------------------

    async def save_embedding(self, row: Embedding) -> None:
        await self._inner.save_embedding(row)

    async def save_embeddings(self, rows: list[Embedding]) -> None:
        await self._inner.save_embeddings(rows)

    async def get_embedding(self, *, canonical_id: str, embedding_model_name: str) -> Embedding | None:
        return await self._inner.get_embedding(canonical_id=canonical_id, embedding_model_name=embedding_model_name)

    async def list_embeddings(self, *, embedding_model_name: str) -> list[Embedding]:
        return await self._inner.list_embeddings(embedding_model_name=embedding_model_name)

    async def get_embeddings_for_canonical_ids(
        self, *, canonical_ids: list[str], embedding_model_name: str,
    ) -> dict[str, Embedding]:
        return await self._inner.get_embeddings_for_canonical_ids(
            canonical_ids=canonical_ids, embedding_model_name=embedding_model_name,
        )

    async def delete_embeddings_for_canonical(self, *, canonical_id_prefix: str) -> int:
        return await self._inner.delete_embeddings_for_canonical(canonical_id_prefix=canonical_id_prefix)
