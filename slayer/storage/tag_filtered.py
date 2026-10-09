"""A per-caller view of a store: whatever the caller's access tags cannot see reads as deleted."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Hashable, Iterable

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import (
    AccessTagsEditError,
    HiddenContentConflictError,
    MemoryNotFoundError,
    UnknownJoinTargetError,
)
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.embeddings.models import Embedding
from slayer.engine.model_reads import Identity, models_read_by
from slayer.memories.models import MEMORY_CANONICAL_PREFIX, Memory
from slayer.storage.base import StorageBackend, _find_case_colliding_id
from slayer.storage.document_loading import DocumentLoadFailures, Loaded


def can_access(model: SlayerModel, *, tags: Iterable[str]) -> bool:
    """Untagged, or sharing a tag with the caller."""
    return not model.access_tags or not set(model.access_tags).isdisjoint(tags)


def entity_model(entity: str) -> Identity | None:
    """``(data_source, model)`` a ``<ds>.<model>[.<leaf>]`` entity names; ``None`` for datasource and memory refs."""
    if entity.startswith(MEMORY_CANONICAL_PREFIX):
        return None
    parts = entity.split(".")
    return (parts[0], parts[1]) if len(parts) >= 2 else None


class _Withheld(BaseModel):
    """The parts of a visible memory its caller does not see."""

    entities: list[str]
    query: SlayerQuery | None = None


class _View(BaseModel):
    """One caller's view of the inner store."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    identities: list[Identity]
    models: dict[Identity, SlayerModel]
    full: dict[Identity, SlayerModel]
    hidden: frozenset[Identity]
    memories: dict[str, Memory]
    hidden_memories: frozenset[str]
    withheld: dict[str, _Withheld]

    def hides(self, canonical_id: str) -> bool:
        if canonical_id.startswith(MEMORY_CANONICAL_PREFIX):
            return canonical_id[len(MEMORY_CANONICAL_PREFIX):] in self.hidden_memories
        return entity_model(canonical_id) in self.hidden

    def pruned(self, model: SlayerModel) -> SlayerModel:
        joins = [j for j in model.joins if (model.data_source, j.target_model) not in self.hidden]
        return model if len(joins) == len(model.joins) else model.model_copy(update={"joins": joins})


def _prior_tags(model: SlayerModel, *, view: _View) -> list[str]:
    """The stored copy's tags; a model moved or copied from another datasource keeps its source's."""
    stored = view.full.get((model.data_source, model.name))
    if stored is not None:
        return stored.access_tags
    sources = [m for (_, name), m in view.full.items() if name == model.name and m.access_tags == model.access_tags]
    return model.access_tags if sources else []


def _visible_identities(*, full: dict[Identity, SlayerModel], tags: frozenset[str]) -> set[Identity]:
    """Accessible models whose reads are all visible, transitively."""
    models = list(full.values())
    reads = {key: models_read_by(m, models=models) - {key} for key, m in full.items()}
    visible = {key for key, m in full.items() if can_access(m, tags=tags)}
    while blocked := {key for key in visible if not reads[key] <= visible}:
        visible -= blocked
    return visible


class TagFilteredStorage(StorageBackend):
    """``backend`` as a caller with ``tags`` sees it; ``bypass=True`` sees and writes everything."""

    def __init__(self, backend: StorageBackend, *, tags: Iterable[str], bypass: bool) -> None:
        self._inner = backend
        self._tags = frozenset(tags)
        self._bypass = bypass
        self._ids_collide_as_filenames = backend._ids_collide_as_filenames
        self._memo: _View | None = None
        self._memo_fingerprint: str | None = None
        self._lock = asyncio.Lock()

    # ---- the view ---------------------------------------------------------

    async def _inner_fingerprint(self) -> str | None:
        try:
            return await self._inner.graph_fingerprint()
        except OSError:
            return None

    async def _view(self) -> _View:
        fingerprint = await self._inner_fingerprint()
        if self._memo is not None and fingerprint is not None and fingerprint == self._memo_fingerprint:
            return self._memo
        async with self._lock:
            before = await self._inner_fingerprint()
            if self._memo is not None and before is not None and before == self._memo_fingerprint:
                return self._memo
            view = await self._build_view()
            # Load-time migration write-back may change the store mid-pass: memoize only a stable pass.
            after = await self._inner_fingerprint()
            self._memo, self._memo_fingerprint = view, (after if after == before else None)
            return view

    async def _build_view(self) -> _View:
        inner = self._inner
        identities = await inner._list_all_model_identities()
        loaded, _ = await inner.load_models()
        full = {(m.data_source, m.name): m for m in loaded}
        visible = _visible_identities(full=full, tags=self._tags)
        hidden = frozenset((set(identities) | set(full)) - visible)
        rows, unloadable = await inner._list_memories_rows(entities=None)
        query_reads = {m.id: models_read_by(m.query, models=loaded) if m.query else frozenset() for m in rows}
        hidden_memories = {e.name for e in unloadable} | {
            m.id for m in rows
            if (linked := {i for e in m.entities if (i := entity_model(e))} | query_reads[m.id]) and linked <= hidden
        }
        view = _View(
            identities=[i for i in identities if i in visible], models={}, full={k: full[k] for k in visible},
            hidden=hidden, memories={}, hidden_memories=frozenset(hidden_memories), withheld={},
        )
        view.models.update((k, view.pruned(full[k])) for k in visible)
        for memory in rows:
            if memory.id in hidden_memories:
                continue
            dropped = [e for e in memory.entities if view.hides(e)]
            withheld_query = memory.query if query_reads[memory.id] & hidden else None
            view.memories[memory.id] = memory.model_copy(update={
                "entities": [e for e in memory.entities if e not in dropped],
                "query": None if withheld_query is not None else memory.query,
            })
            if dropped or withheld_query is not None:
                view.withheld[memory.id] = _Withheld(entities=dropped, query=withheld_query)
        return view

    # ---- cache keys ---------------------------------------------------------

    async def graph_fingerprint(self) -> str | None:
        fingerprint = await self._inner.graph_fingerprint()
        if self._bypass or fingerprint is None:
            return fingerprint
        return json.dumps([fingerprint, sorted(self._tags)])

    async def cache_identity(self) -> Hashable:
        inner = await self._inner.cache_identity()
        return inner if self._bypass else (inner, self._tags, False)

    async def aclose(self) -> None:
        close = getattr(self._inner, "aclose", None)
        if close is not None:
            await close()

    # ---- models -------------------------------------------------------------

    async def _list_all_model_identities(self) -> list[tuple[str, str]]:
        if self._bypass:
            return await self._inner._list_all_model_identities()
        return list((await self._view()).identities)

    async def get_model(self, name: str, data_source: str | None = None) -> SlayerModel | None:
        if self._bypass:
            return await self._inner.get_model(name, data_source=data_source)
        target = await self._resolve_target_or_none(name, data_source=data_source)
        model = (await self._view()).models.get(target) if target is not None else None
        return model.model_copy(deep=True) if model is not None else None

    async def _load_raw_model_dict(self, *, name: str, data_source: str) -> dict | None:
        if self._bypass:
            return await self._inner._load_raw_model_dict(name=name, data_source=data_source)
        return await super()._load_raw_model_dict(name=name, data_source=data_source)

    async def save_model(
        self, model: SlayerModel, *, _validate: bool = True, failures: DocumentLoadFailures | None = None,
    ) -> None:
        """Bypass: the inner save. Otherwise validate in the view, then save the model merged onto its full stored copy."""
        if self._bypass:
            await self._inner.save_model(model, _validate=_validate, failures=failures)
            return
        view = await self._view()
        if model.access_tags != _prior_tags(model, view=view):
            raise AccessTagsEditError(model=model.name)
        if _validate:
            for join in model.joins:
                if (model.data_source, join.target_model) not in view.models:
                    raise UnknownJoinTargetError(model=model.name, target=join.target_model)
            await self.validate_for_save(model, failures=failures)
        merged = self._merged(model, view=view)
        try:
            if merged is not model:
                merged = SlayerModel.model_validate(merged.model_dump())
            await self._inner.save_model(merged, _validate=_validate)
        except Exception:  # noqa: BLE001 — valid in the view, invalid merged: the cause names hidden parts
            raise HiddenContentConflictError(kind="model", name=model.name) from None

    def _merged(self, model: SlayerModel, *, view: _View) -> SlayerModel:
        key = (model.data_source, model.name)
        if key in view.hidden:
            raise HiddenContentConflictError(kind="model", name=model.name)
        stored = view.full.get(key)
        if stored is None:
            if any(n == model.name and m.joins != view.models[(ds, n)].joins for (ds, n), m in view.full.items()):
                raise HiddenContentConflictError(kind="model", name=model.name)  # a move would drop its pruned joins
            return model
        pruned = [j for j in stored.joins if (model.data_source, j.target_model) in view.hidden]
        return model.model_copy(update={"joins": [*model.joins, *pruned]}) if pruned else model

    async def _save_model_impl(self, model: SlayerModel) -> None:
        if self._bypass:
            await self._inner._save_model_impl(model)
            return
        await self.save_model(model, _validate=False)

    async def _visible(self, *, data_source: str, name: str) -> bool:
        return (data_source, name) in (await self._view()).models

    async def delete_model(self, name: str, data_source: str | None = None) -> bool:
        if self._bypass:
            return await self._inner.delete_model(name, data_source=data_source)
        target = await self._resolve_target_or_none(name, data_source=data_source)
        if target is None or not await self._visible(data_source=target[0], name=target[1]):
            return False
        return await self._inner.delete_model(target[1], data_source=target[0])

    async def _delete_model_row(self, *, data_source: str, name: str) -> bool:
        if not self._bypass and not await self._visible(data_source=data_source, name=name):
            return False
        return await self._inner._delete_model_row(data_source=data_source, name=name)

    async def update_column_sampled(
        self, *, data_source: str, model_name: str, column_name: str,
        sampled: str | None, sampled_values: list[str] | None, distinct_count: int | None,
    ) -> None:
        if not self._bypass and not await self._visible(data_source=data_source, name=model_name):
            raise ValueError(f"update_column_sampled: model {model_name!r} in datasource {data_source!r} not found.")
        await self._inner.update_column_sampled(
            data_source=data_source, model_name=model_name, column_name=column_name,
            sampled=sampled, sampled_values=sampled_values, distinct_count=distinct_count,
        )

    # ---- datasources (not filtered) ----------------------------------------

    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        await self._inner._save_datasource_impl(datasource)

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
        return await self._inner.get_datasource(name)

    async def list_datasources(self) -> list[str]:
        return await self._inner.list_datasources()

    async def delete_datasource(self, name: str) -> bool:
        return await self._inner.delete_datasource(name)

    async def _delete_datasource_row(self, name: str) -> bool:
        return await self._inner._delete_datasource_row(name)

    async def get_datasource_priority(self) -> list[str]:
        return await self._inner.get_datasource_priority()

    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        await self._inner._set_datasource_priority_raw(priority)

    # ---- memories -------------------------------------------------------------

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        if self._bypass:
            return await self._inner._get_memory_row(memory_id)
        memory = (await self._view()).memories.get(memory_id)
        return memory.model_copy(deep=True) if memory is not None else None

    async def _list_memories_rows(self, *, entities: list[str] | None) -> Loaded[Memory]:
        if self._bypass:
            return await self._inner._list_memories_rows(entities=entities)
        wanted = set(entities) if entities is not None else None
        return [
            m.model_copy(deep=True) for m in (await self._view()).memories.values()
            if wanted is None or wanted & set(m.entities)
        ], []

    async def _memory_ids(self) -> list[str]:
        if self._bypass:
            return await self._inner._memory_ids()
        return list((await self._view()).memories)

    async def _next_memory_seq(self) -> str:
        return await self._inner._next_memory_seq()

    async def save_memory(
        self,
        *,
        learning: str,
        entities: list[str],
        query: SlayerQuery | None = None,
        id: str | None = None,  # noqa: A002 — mirrors the base signature
        description: str | None = None,
    ) -> Memory:
        if self._bypass or id is None:
            return await self._inner.save_memory(
                learning=learning, entities=entities, query=query, id=id, description=description,
            )
        return await super().save_memory(
            learning=learning, entities=entities, query=query, id=id, description=description,
        )

    async def _save_memory_row(self, memory: Memory) -> None:
        """Bypass: the inner write. Otherwise the memory keeps its withheld entities and query."""
        if self._bypass:
            await self._inner._save_memory_row(memory)
            return
        view = await self._view()
        if memory.id in view.hidden_memories or (
            self._ids_collide_as_filenames
            and _find_case_colliding_id(memory.id, view.hidden_memories) is not None
        ):
            raise HiddenContentConflictError(kind="memory", name=memory.id)
        withheld = view.withheld.get(memory.id)
        if withheld is not None:
            memory = memory.model_copy(update={
                "entities": [*memory.entities, *(e for e in withheld.entities if e not in memory.entities)],
                "query": memory.query if memory.query is not None else withheld.query,
            })
        await self._inner._save_memory_row(memory)

    async def delete_memory(self, memory_id: str) -> None:
        if not self._bypass and memory_id not in (await self._view()).memories:
            raise MemoryNotFoundError(memory_id)
        await self._inner.delete_memory(memory_id)

    async def _delete_memory_row(self, memory_id: str) -> bool:
        if not self._bypass and memory_id not in (await self._view()).memories:
            return False
        return await self._inner._delete_memory_row(memory_id)

    async def strip_dangling_entities_from_memories(self, *, canonical_id: str) -> int:
        """The inner cascade over full documents; counts only the caller's memories."""
        if self._bypass:
            return await self._inner.strip_dangling_entities_from_memories(canonical_id=canonical_id)
        view = await self._view()
        if not canonical_id or view.hides(canonical_id):
            return 0
        is_memory_ref = canonical_id.startswith(MEMORY_CANONICAL_PREFIX)
        visible = sum(
            1 for m in view.memories.values()
            if self._memory_has_cascade_candidate(memory=m, canonical_id=canonical_id, is_memory_ref=is_memory_ref)
        )
        await self._inner.strip_dangling_entities_from_memories(canonical_id=canonical_id)
        return visible

    # ---- embeddings -------------------------------------------------------------

    async def _refuse_hidden(self, rows: list[Embedding]) -> None:
        if self._bypass:
            return
        view = await self._view()
        hidden = next((r for r in rows if view.hides(r.canonical_id)), None)
        if hidden is not None:
            raise HiddenContentConflictError(kind="embedding", name=hidden.canonical_id)

    async def save_embedding(self, row: Embedding) -> None:
        await self._refuse_hidden([row])
        await self._inner.save_embedding(row)

    async def save_embeddings(self, rows: list[Embedding]) -> None:
        await self._refuse_hidden(list(rows))
        await self._inner.save_embeddings(rows)

    async def _hides(self, canonical_id: str) -> bool:
        return not self._bypass and (await self._view()).hides(canonical_id)

    async def get_embedding(self, *, canonical_id: str, embedding_model_name: str) -> Embedding | None:
        if await self._hides(canonical_id):
            return None
        return await self._inner.get_embedding(canonical_id=canonical_id, embedding_model_name=embedding_model_name)

    async def list_embeddings(self, *, embedding_model_name: str) -> list[Embedding]:
        rows = await self._inner.list_embeddings(embedding_model_name=embedding_model_name)
        if self._bypass:
            return rows
        view = await self._view()
        return [r for r in rows if not view.hides(r.canonical_id)]

    async def get_embeddings_for_canonical_ids(
        self, *, canonical_ids: list[str], embedding_model_name: str,
    ) -> dict[str, Embedding]:
        if not self._bypass:
            view = await self._view()
            canonical_ids = [c for c in canonical_ids if not view.hides(c)]
        return await self._inner.get_embeddings_for_canonical_ids(
            canonical_ids=canonical_ids, embedding_model_name=embedding_model_name,
        )

    async def delete_embeddings_for_canonical(self, *, canonical_id_prefix: str) -> int:
        if await self._hides(canonical_id_prefix):
            return 0
        return await self._inner.delete_embeddings_for_canonical(canonical_id_prefix=canonical_id_prefix)

