"""A query engine's non-owning view of its store: loads that need the live datasource read it through the engine."""

from __future__ import annotations

from collections.abc import Awaitable, Hashable
from typing import Any, TypeVar

from slayer.core.semantic_model import ModelScopedMetric
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.embeddings.models import Embedding
from slayer.memories.models import Memory
from slayer.storage.base import CLIENT_SOURCE, ClientSource, StorageBackend
from slayer.storage.document_loading import DocumentLoadFailures, Loaded
from slayer.storage.ontology_documents import StoredMapping

_T = TypeVar("_T")


class EngineStorage(StorageBackend):
    """``inner`` as one query engine reads it; closing the view never closes ``inner``."""

    _delegates_documents = True

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

    async def _sourced(self, call: Awaitable[_T]) -> _T:
        """Await a delegated call with this engine as the loading engine."""
        token = CLIENT_SOURCE.set(self._client_source)
        try:
            return await call
        finally:
            CLIENT_SOURCE.reset(token)

    # ---- public surface: the store's own behaviour (access filtering included), sourced -------

    async def save_dataset(
        self, dataset: SlayerDataset, *, _validate: bool = True, failures: DocumentLoadFailures | None = None,
    ) -> None:
        await self._sourced(self._inner.save_dataset(dataset, _validate=_validate, failures=failures))

    async def validate_for_save(self, dataset: SlayerDataset, *, failures: DocumentLoadFailures | None = None) -> None:
        await self._sourced(self._inner.validate_for_save(dataset, failures=failures))

    async def builtin_datasets(
        self, data_source: str, *, detailed: bool = False, failures: DocumentLoadFailures | None = None,
    ) -> list[SlayerDataset]:
        return await self._sourced(self._inner.builtin_datasets(data_source, detailed=detailed, failures=failures))

    async def load_datasets(self, *, data_source: str | None = None, exclude: str | None = None) -> Loaded[SlayerDataset]:
        return await self._sourced(self._inner.load_datasets(data_source=data_source, exclude=exclude))

    async def get_dataset_or_builtin(self, name: str, data_source: str | None = None) -> SlayerDataset | None:
        return await self._sourced(self._inner.get_dataset_or_builtin(name, data_source=data_source))

    async def delete_dataset(self, name: str, data_source: str | None = None) -> bool:
        return await self._sourced(self._inner.delete_dataset(name, data_source=data_source))

    async def save_datasource(self, datasource: DatasourceConfig) -> None:
        await self._sourced(self._inner.save_datasource(datasource))

    async def check_datasource_id_collision(self, name: str) -> None:
        await self._sourced(self._inner.check_datasource_id_collision(name))

    async def delete_datasource(self, name: str) -> bool:
        return await self._sourced(self._inner.delete_datasource(name))

    async def set_datasource_priority(self, priority: list[str]) -> None:
        await self._sourced(self._inner.set_datasource_priority(priority))

    async def list_datasets(self, data_source: str | None = None) -> list[str]:
        return await self._sourced(self._inner.list_datasets(data_source=data_source))

    async def resolve_dataset_identity(self, name: str, *, prefer_data_source: str | None = None) -> tuple[str, str] | None:
        return await self._sourced(self._inner.resolve_dataset_identity(name, prefer_data_source=prefer_data_source))

    async def get_memory_row(self, memory_id: str) -> Memory | None:
        return await self._sourced(self._inner.get_memory_row(memory_id))

    async def get_memory(self, memory_id: str) -> Memory:
        return await self._sourced(self._inner.get_memory(memory_id))

    async def load_memories(self, *, entities: list[str] | None = None) -> Loaded[Memory]:
        return await self._sourced(self._inner.load_memories(entities=entities))

    async def list_memories(
        self, *, entities: list[str] | None = None, failures: DocumentLoadFailures | None = None,
    ) -> list[Memory]:
        return await self._sourced(self._inner.list_memories(entities=entities, failures=failures))

    async def delete_memory(self, memory_id: str) -> None:
        await self._sourced(self._inner.delete_memory(memory_id))

    # ---- datasets -----------------------------------------------------------

    # ---- semantic-model documents (the inner store's) ------------------------

    async def list_semantic_models(self, data_source: str | None = None) -> list[tuple[str, str]]:
        return await self._inner.list_semantic_models(data_source=data_source)

    async def get_semantic_model_text(self, name: str, data_source: str | None = None) -> str | None:
        return await self._inner.get_semantic_model_text(name, data_source=data_source)

    async def save_semantic_model_text(self, *, data_source: str, name: str, text: str) -> None:
        await self._inner.save_semantic_model_text(data_source=data_source, name=name, text=text)

    async def delete_semantic_model(self, name: str, data_source: str | None = None) -> bool:
        return await self._inner.delete_semantic_model(name, data_source=data_source)

    async def semantic_model_owner(self, name: str, data_source: str) -> str | None:
        return await self._inner.semantic_model_owner(name, data_source=data_source)

    async def save_dataset_extensions(self, dataset: SlayerDataset) -> None:
        await self._sourced(self._inner.save_dataset_extensions(dataset))

    async def adopt_semantic_model(self, *, data_source: str, name: str, text: str, replacing: list[str]) -> None:
        await self._sourced(self._inner.adopt_semantic_model(data_source=data_source, name=name, text=text, replacing=replacing))

    async def create_semantic_model(self, *, data_source: str, name: str | None = None, content: str | dict[str, Any] | None = None,
                                    datasets: list[str] | None = None) -> None:
        await self._sourced(self._inner.create_semantic_model(data_source=data_source, name=name, content=content, datasets=datasets))

    async def edit_semantic_model(self, name: str, *, data_source: str, **edits: Any) -> None:
        await self._sourced(self._inner.edit_semantic_model(name, data_source=data_source, **edits))

    async def list_model_scoped_metrics(self, data_source: str) -> list[ModelScopedMetric]:
        return await self._inner.list_model_scoped_metrics(data_source)

    async def list_ontologies(self) -> list[str]:
        return await self._inner.list_ontologies()

    async def get_ontology_text(self, name: str) -> str | None:
        return await self._inner.get_ontology_text(name)

    async def save_ontology_text(self, *, name: str, text: str) -> None:
        await self._sourced(self._inner.save_ontology_text(name=name, text=text))

    async def delete_ontology(self, name: str) -> bool:
        return await self._sourced(self._inner.delete_ontology(name))

    async def edit_ontology(self, name: str, **edits: Any) -> None:
        await self._sourced(self._inner.edit_ontology(name, **edits))

    async def list_ontology_mappings(self, data_source: str | None = None) -> list[tuple[str, str]]:
        return await self._inner.list_ontology_mappings(data_source)

    async def get_ontology_mapping_text(self, name: str, data_source: str) -> str | None:
        return await self._inner.get_ontology_mapping_text(name, data_source)

    async def save_ontology_mapping_text(self, *, data_source: str, name: str, text: str) -> list[str]:
        return await self._sourced(self._inner.save_ontology_mapping_text(data_source=data_source, name=name,
                                                                          text=text))

    async def edit_ontology_mapping(self, name: str, *, data_source: str, **edits: Any) -> list[str]:
        return await self._sourced(self._inner.edit_ontology_mapping(name, data_source=data_source, **edits))

    async def delete_ontology_mapping(self, name: str, data_source: str) -> bool:
        return await self._inner.delete_ontology_mapping(name, data_source)

    async def ontology_mapping_problems(self, name: str, data_source: str) -> list[str]:
        return await self._sourced(self._inner.ontology_mapping_problems(name, data_source))

    async def load_ontology_mappings(self, *, data_source: str | None = None) -> Loaded[StoredMapping]:
        return await self._sourced(self._inner.load_ontology_mappings(data_source=data_source))

    async def get_dataset(self, name: str, data_source: str | None = None) -> SlayerDataset | None:
        return await self._sourced(self._inner.get_dataset(name, data_source=data_source))

    async def _list_all_dataset_identities(self) -> list[tuple[str, str]]:
        return await self._sourced(self._inner._list_all_dataset_identities())

    async def _load_raw_dataset_dict(self, *, name: str, data_source: str) -> dict | None:
        return await self._sourced(self._inner._load_raw_dataset_dict(name=name, data_source=data_source))

    async def _save_dataset_impl(self, dataset: SlayerDataset) -> None:
        await self._sourced(self._inner._save_dataset_impl(dataset))

    async def _delete_dataset_row(self, *, data_source: str, name: str) -> bool:
        return await self._sourced(self._inner._delete_dataset_row(data_source=data_source, name=name))

    async def update_field_sampled(
        self, *, data_source: str, dataset_name: str, field_name: str,
        sampled: str | None, sampled_values: list[str] | None, distinct_count: int | None,
    ) -> None:
        await self._sourced(self._inner.update_field_sampled(
            data_source=data_source, dataset_name=dataset_name, field_name=field_name,
            sampled=sampled, sampled_values=sampled_values, distinct_count=distinct_count,
        ))

    async def graph_fingerprint(self) -> str | None:
        return await self._sourced(self._inner.graph_fingerprint())

    async def cache_identity(self) -> Hashable:
        return await self._sourced(self._inner.cache_identity())

    # ---- datasources --------------------------------------------------------

    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        await self._sourced(self._inner._save_datasource_impl(datasource))

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
        return await self._sourced(self._inner.get_datasource(name))

    async def list_datasources(self) -> list[str]:
        return await self._sourced(self._inner.list_datasources())

    async def _delete_datasource_row(self, name: str) -> bool:
        return await self._sourced(self._inner._delete_datasource_row(name))

    async def get_datasource_priority(self) -> list[str]:
        return await self._sourced(self._inner.get_datasource_priority())

    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        await self._sourced(self._inner._set_datasource_priority_raw(priority))

    # ---- memories -----------------------------------------------------------

    async def _save_memory_row(self, memory: Memory) -> None:
        await self._sourced(self._inner._save_memory_row(memory))

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        return await self._sourced(self._inner._get_memory_row(memory_id))

    async def _list_memories_rows(self, *, entities: list[str] | None) -> Loaded[Memory]:
        return await self._sourced(self._inner._list_memories_rows(entities=entities))

    async def _delete_memory_row(self, memory_id: str) -> bool:
        return await self._sourced(self._inner._delete_memory_row(memory_id))

    async def _next_memory_seq(self) -> str:
        return await self._sourced(self._inner._next_memory_seq())

    async def _memory_ids(self) -> list[str]:
        return await self._sourced(self._inner._memory_ids())

    async def save_memory(
        self,
        *,
        learning: str,
        entities: list[str],
        query: SlayerQuery | None = None,
        id: str | None = None,  # noqa: A002 — mirrors the base signature
        description: str | None = None,
    ) -> Memory:
        return await self._sourced(self._inner.save_memory(
            learning=learning, entities=entities, query=query, id=id, description=description,
        ))

    async def strip_dangling_entities_from_memories(self, *, canonical_id: str) -> int:
        return await self._sourced(self._inner.strip_dangling_entities_from_memories(canonical_id=canonical_id))

    # ---- embeddings ---------------------------------------------------------

    async def save_embedding(self, row: Embedding) -> None:
        await self._sourced(self._inner.save_embedding(row))

    async def save_embeddings(self, rows: list[Embedding]) -> None:
        await self._sourced(self._inner.save_embeddings(rows))

    async def get_embedding(self, *, canonical_id: str, embedding_model_name: str) -> Embedding | None:
        return await self._sourced(self._inner.get_embedding(canonical_id=canonical_id, embedding_model_name=embedding_model_name))

    async def list_embeddings(self, *, embedding_model_name: str) -> list[Embedding]:
        return await self._sourced(self._inner.list_embeddings(embedding_model_name=embedding_model_name))

    async def get_embeddings_for_canonical_ids(
        self, *, canonical_ids: list[str], embedding_model_name: str,
    ) -> dict[str, Embedding]:
        return await self._sourced(self._inner.get_embeddings_for_canonical_ids(
            canonical_ids=canonical_ids, embedding_model_name=embedding_model_name,
        ))

    async def delete_embeddings_for_canonical(self, *, canonical_id_prefix: str) -> int:
        return await self._sourced(self._inner.delete_embeddings_for_canonical(canonical_id_prefix=canonical_id_prefix))
