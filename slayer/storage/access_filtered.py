"""A per-caller view of a store: whatever the caller's access tags cannot see reads as deleted."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Hashable, Iterable
from typing import Any

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import (
    AccessTagsEditError,
    HiddenContentConflictError,
    MemoryNotFoundError,
    UnknownRelationshipTargetError,
)
from slayer.core.document_text import edit_text
from slayer.core.field_expression import Expression
from slayer.core.semantic_model import ModelScopedMetric
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.embeddings.models import Embedding
from slayer.engine.dataset_reads import Identity, datasets_read_by, placeholder
from slayer.memories.models import MEMORY_CANONICAL_PREFIX, Memory
from slayer.storage.base import StorageBackend, _find_case_colliding_id, check_relationship_target_spellings
from slayer.storage.document_loading import DocumentLoadFailures, Loaded
from slayer.storage.document_mapping import as_expression, document_payload, with_document_payload
from slayer.storage.model_metric_rules import metric_references, visible_model_metrics
from slayer.storage.semantic_model_edits import metric_spec
from slayer.core.document_text import parse_document_text

_SEMANTIC_MODEL = "semantic model"

def can_access(dataset: SlayerDataset, *, tags: Iterable[str]) -> bool:
    """Untagged, or sharing a tag with the caller."""
    return not dataset.access_tags or not set(dataset.access_tags).isdisjoint(tags)


def entity_dataset(entity: str) -> Identity | None:
    """``(data_source, dataset)`` a ``<ds>.<dataset>[.<leaf>]`` entity names; ``None`` for datasource and memory refs."""
    if entity.startswith(MEMORY_CANONICAL_PREFIX):
        return None
    parts = entity.split(".")
    return (parts[0], parts[1]) if len(parts) >= 2 else None


def _fold(name: Any) -> str:
    return str(name).casefold()


def _entities(content: str | dict[str, Any] | None) -> dict[str, Any]:
    doc = parse_document_text(content) if isinstance(content, str) else content
    return doc if isinstance(doc, dict) else {}


def _declared_names(entities: dict[str, Any]) -> list[Any]:
    core = [e.get("name") for key in ("datasets", "metrics") for e in entities.get(key) or [] if isinstance(e, dict)]
    return [*core, *(m.name for m in document_payload(entities).metrics)]


class _HiddenEntities(BaseModel):
    """One datasource's datasets and model-scoped metrics a caller cannot access, case-folded."""

    datasets: set[str]
    metrics: set[str]

    def hides(self, key: str, entity: dict[str, Any]) -> bool:
        if key == "relationships":
            return _touches(entity, hidden=self)
        return _fold(entity.get("name")) in (self.datasets if key == "datasets" else self.metrics)

    def held_by(self, text: str) -> bool:
        doc = _entities(text)
        return any(self.hides(key, e) for key in _ENTITY_KEYS for e in doc.get(key) or []) or any(
            _fold(m.name) in self.metrics for m in document_payload(doc).metrics)


_ENTITY_KEYS = ("datasets", "relationships", "metrics")


def _touches(relationship: dict[str, Any], *, hidden: _HiddenEntities) -> bool:
    return bool({_fold(relationship.get("from")), _fold(relationship.get("to"))} & hidden.datasets)


def _without_hidden(doc: dict[str, Any], *, hidden: _HiddenEntities) -> dict[str, Any]:
    out = dict(doc)
    for key in _ENTITY_KEYS:
        if key in doc:
            out[key] = [e for e in doc[key] or [] if not hidden.hides(key, e)]
    payload = document_payload(doc)
    visible = [m for m in payload.metrics if _fold(m.name) not in hidden.metrics]
    if len(visible) != len(payload.metrics):
        with_document_payload(out, payload.model_copy(update={"metrics": visible}))
    return out


def _reinserted(items: list[Any], *, hidden: list[tuple[int, Any]]) -> list[Any]:
    merged = list(items)
    for index, item in hidden:
        merged.insert(min(index, len(merged)), item)
    return merged


def _with_hidden(doc: dict[str, Any], *, full: dict[str, Any], hidden: _HiddenEntities) -> dict[str, Any]:
    """``doc`` with ``full``'s hidden entities put back at their stored positions."""
    out = dict(doc)
    for key in _ENTITY_KEYS:
        kept = [(i, e) for i, e in enumerate(full.get(key) or []) if hidden.hides(key, e)]
        if kept:
            out[key] = _reinserted(doc.get(key) or [], hidden=kept)
    kept_metrics = [(i, m) for i, m in enumerate(document_payload(full).metrics) if _fold(m.name) in hidden.metrics]
    if kept_metrics:
        payload = document_payload(out)
        with_document_payload(out, payload.model_copy(update={
            "metrics": _reinserted(payload.metrics, hidden=kept_metrics)}))
    return out


class _Withheld(BaseModel):
    """The parts of a visible memory its caller does not see."""

    entities: list[str]
    query: SlayerQuery | None = None


class _View(BaseModel):
    """One caller's view of the inner store."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    identities: list[Identity]
    datasets: dict[Identity, SlayerDataset]
    full: dict[Identity, SlayerDataset]
    hidden: frozenset[Identity]
    memories: dict[str, Memory]
    hidden_memories: frozenset[str]
    withheld: dict[str, _Withheld]

    def hides(self, canonical_id: str) -> bool:
        if canonical_id.startswith(MEMORY_CANONICAL_PREFIX):
            return canonical_id[len(MEMORY_CANONICAL_PREFIX):] in self.hidden_memories
        return entity_dataset(canonical_id) in self.hidden

    def hides_under(self, prefix: str) -> bool:
        """Whether a hidden dataset or memory id is ``prefix`` or descends from it."""
        hidden = [f"{ds}.{name}" for ds, name in self.hidden]
        hidden += [f"{MEMORY_CANONICAL_PREFIX}{m}" for m in self.hidden_memories]
        return any(cid == prefix or cid.startswith(f"{prefix}.") for cid in hidden)

    def pruned(self, dataset: SlayerDataset) -> SlayerDataset:
        relationships = [j for j in dataset.relationships if (dataset.data_source, j.to) not in self.hidden]
        return dataset if len(relationships) == len(dataset.relationships) else dataset.model_copy(update={"relationships": relationships})


def _prior_tags(dataset: SlayerDataset, *, view: _View) -> list[str]:
    """The stored copy's tags; a new dataset, moved or copied ones included, has none."""
    stored = view.full.get((dataset.data_source, dataset.name))
    return stored.access_tags if stored is not None else []


def _visible_identities(
    *, full: dict[Identity, SlayerDataset], known: list[SlayerDataset], tags: frozenset[str],
) -> set[Identity]:
    """Accessible datasets whose reads, resolved over ``known`` (loaded plus placeholders), are all visible, transitively."""
    reads = {key: datasets_read_by(m, datasets=known) - {key} for key, m in full.items()}
    visible = {key for key, m in full.items() if can_access(m, tags=tags)}
    while blocked := {key for key in visible if not reads[key] <= visible}:
        visible -= blocked
    return visible


class AccessFilteredStorage(StorageBackend):
    """``backend`` as a caller with ``tags`` sees it; ``bypass=True`` sees and writes everything."""

    _delegates_documents = True

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
        identities = await inner._list_all_dataset_identities()
        loaded, _ = await inner.load_datasets()
        full = {(m.data_source, m.name): m for m in loaded}
        known = [*loaded, *(placeholder(i) for i in identities if i not in full)]
        visible = _visible_identities(full=full, known=known, tags=self._tags)
        hidden = frozenset((set(identities) | set(full)) - visible)
        rows, unloadable = await inner._list_memories_rows(entities=None)
        query_reads = {m.id: datasets_read_by(m.query, datasets=known) if m.query else frozenset() for m in rows}
        hidden_memories = {e.name for e in unloadable} | {
            m.id for m in rows
            if (linked := {i for e in m.entities if (i := entity_dataset(e))} | query_reads[m.id]) and linked <= hidden
        }
        view = _View(
            identities=[i for i in identities if i in visible], datasets={}, full={k: full[k] for k in visible},
            hidden=hidden, memories={}, hidden_memories=frozenset(hidden_memories), withheld={},
        )
        view.datasets.update((k, view.pruned(full[k])) for k in visible)
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

    # ---- datasets -----------------------------------------------------------

    async def _list_all_dataset_identities(self) -> list[tuple[str, str]]:
        if self._bypass:
            return await self._inner._list_all_dataset_identities()
        return list((await self._view()).identities)

    # ---- semantic-model documents: hidden entities read as absent, writes keep them --

    async def _hidden_entities(self, data_source: str) -> _HiddenEntities:
        view = await self._view()
        datasets = {n.casefold() for ds, n in view.hidden if ds == data_source}
        metrics = await self._inner.list_model_scoped_metrics(data_source)
        visible = {m.name.casefold() for m in visible_model_metrics(metrics, hidden_datasets=datasets)}
        return _HiddenEntities(datasets=datasets, metrics={m.name.casefold() for m in metrics} - visible)

    async def _refuse_hidden_names(self, *, data_source: str, document: str, datasets: Iterable[Any] = (),
                                   metrics: Iterable[dict[str, Any]] = (), relationships: Iterable[dict[str, Any]] = (),
                                   names: Iterable[Any] = (), expressions: Iterable[Expression] = ()) -> None:
        """Refuse a write naming or reading an entity the caller cannot access."""
        hidden = await self._hidden_entities(data_source)
        metrics = list(metrics)
        read = {_fold(d) for d in datasets}
        read |= {_fold(r.get(end)) for r in relationships for end in ("from", "to")}
        named = {_fold(n) for n in names} | {_fold(m.get("name")) for m in metrics}
        for expression in [*expressions, *(as_expression(metric_spec(m)["expression"]) for m in metrics)]:
            read_datasets, read_metrics = metric_references(expression)
            read |= read_datasets
            named |= read_metrics
        if read & hidden.datasets or named & (hidden.datasets | hidden.metrics):
            raise HiddenContentConflictError(kind=_SEMANTIC_MODEL, name=document)

    async def _refuse_hidden_in_document(self, *, data_source: str, document: str, entities: dict[str, Any],
                                         datasets: Iterable[Any] = ()) -> None:
        """Refuse a whole document that names or reads, in any metric, an entity the caller cannot access."""
        await self._refuse_hidden_names(
            data_source=data_source, document=document, datasets=datasets, names=_declared_names(entities),
            relationships=entities.get("relationships") or [],
            metrics=[m for m in entities.get("metrics") or [] if isinstance(m, dict)],
            expressions=[m.expression for m in document_payload(entities).metrics])

    async def _stored_document(self, name: str, data_source: str | None) -> tuple[str, str, str] | None:
        target = await self._inner._resolve_document(name, data_source)
        text = await self._inner.get_semantic_model_text(target[1], data_source=target[0]) if target else None
        return (target[0], target[1], text) if target and text is not None else None

    async def list_semantic_models(self, data_source: str | None = None) -> list[tuple[str, str]]:
        return await self._inner.list_semantic_models(data_source=data_source)

    async def get_semantic_model_text(self, name: str, data_source: str | None = None) -> str | None:
        if self._bypass:
            return await self._inner.get_semantic_model_text(name, data_source=data_source)
        stored = await self._stored_document(name, data_source)
        if stored is None:
            return None
        ds, _, text = stored
        hidden = await self._hidden_entities(ds)
        return edit_text(text, lambda doc: _without_hidden(doc, hidden=hidden)) if hidden.held_by(text) else text

    async def save_semantic_model_text(self, *, data_source: str, name: str, text: str) -> None:
        if not self._bypass:
            entities = _entities(text)
            await self._refuse_hidden_in_document(data_source=data_source, document=name, entities=entities)
            stored =await self._stored_document(name, data_source)
            hidden = await self._hidden_entities(data_source)
            if stored is not None and hidden.held_by(stored[2]):
                full = _entities(stored[2])
                text = edit_text(stored[2], lambda _doc: _with_hidden(entities, full=full, hidden=hidden))
        await self._inner.save_semantic_model_text(data_source=data_source, name=name, text=text)

    async def delete_semantic_model(self, name: str, data_source: str | None = None) -> bool:
        if not self._bypass:
            stored = await self._stored_document(name, data_source)
            if stored is None:
                return False
            if (await self._hidden_entities(stored[0])).held_by(stored[2]):
                raise HiddenContentConflictError(kind=_SEMANTIC_MODEL, name=stored[1])
        return await self._inner.delete_semantic_model(name, data_source=data_source)

    async def semantic_model_owner(self, name: str, data_source: str) -> str | None:
        if not self._bypass and name.casefold() in (await self._hidden_entities(data_source)).datasets:
            return None
        return await self._inner.semantic_model_owner(name, data_source=data_source)

    async def save_dataset_extensions(self, dataset: SlayerDataset) -> None:
        if not self._bypass and dataset.name.casefold() in (await self._hidden_entities(dataset.data_source)).datasets:
            raise HiddenContentConflictError(kind="dataset", name=dataset.name)
        await self._inner.save_dataset_extensions(dataset)

    async def adopt_semantic_model(self, *, data_source: str, name: str, text: str, replacing: list[str]) -> None:
        if not self._bypass:
            await self._refuse_hidden_in_document(data_source=data_source, document=name, entities=_entities(text),
                                                  datasets=replacing)
        await self._inner.adopt_semantic_model(data_source=data_source, name=name, text=text, replacing=replacing)

    async def create_semantic_model(self, *, data_source: str, name: str | None = None, content: str | dict[str, Any] | None = None,
                                    datasets: list[str] | None = None) -> None:
        if not self._bypass:
            entities = _entities(content)
            await self._refuse_hidden_in_document(data_source=data_source, document=str(name or entities.get("name")),
                                                  entities=entities, datasets=datasets or [])
        await self._inner.create_semantic_model(data_source=data_source, name=name, content=content, datasets=datasets)

    async def edit_semantic_model(self, name: str, *, data_source: str, **edits: Any) -> None:
        if not self._bypass:
            stored = await self._stored_document(name, data_source)
            hidden = await self._hidden_entities(data_source)
            relationships = edits.get("relationships") or []
            hidden_relationships = {_fold(r.get("name")) for r in _entities(stored[2] if stored else None).get(
                "relationships") or [] if _touches(r, hidden=hidden)}
            touched = [*(edits.get("remove_relationships") or []), *(r.get("name") for r in relationships)]
            if {_fold(n) for n in touched} & hidden_relationships:
                raise HiddenContentConflictError(kind=_SEMANTIC_MODEL, name=name)
            await self._refuse_hidden_names(data_source=data_source, document=name, relationships=relationships,
                                            datasets=edits.get("add_datasets") or [], metrics=edits.get("metrics") or [],
                                            names=edits.get("remove_metrics") or [])
        await self._inner.edit_semantic_model(name, data_source=data_source, **edits)

    async def list_model_scoped_metrics(self, data_source: str) -> list[ModelScopedMetric]:
        metrics = await self._inner.list_model_scoped_metrics(data_source)
        if self._bypass:
            return metrics
        view = await self._view()
        return visible_model_metrics(metrics, hidden_datasets={n.casefold() for ds, n in view.hidden if ds == data_source})

    async def get_dataset(self, name: str, data_source: str | None = None) -> SlayerDataset | None:
        if self._bypass:
            return await self._inner.get_dataset(name, data_source=data_source)
        target = await self._resolve_target_or_none(name, data_source=data_source)
        dataset = (await self._view()).datasets.get(target) if target is not None else None
        return dataset.model_copy(deep=True) if dataset is not None else None

    async def _load_raw_dataset_dict(self, *, name: str, data_source: str) -> dict | None:
        if self._bypass:
            return await self._inner._load_raw_dataset_dict(name=name, data_source=data_source)
        return await super()._load_raw_dataset_dict(name=name, data_source=data_source)

    async def save_dataset(
        self, dataset: SlayerDataset, *, _validate: bool = True, failures: DocumentLoadFailures | None = None,
    ) -> None:
        """Bypass: the inner save. Otherwise validate in the view, then save the dataset merged onto its full stored copy."""
        if self._bypass:
            await self._inner.save_dataset(dataset, _validate=_validate, failures=failures)
            return
        view = await self._view()
        if dataset.access_tags != _prior_tags(dataset, view=view):
            raise AccessTagsEditError(dataset=dataset.name)
        if _validate:
            names = [n for ds, n in view.identities if ds == dataset.data_source]
            check_relationship_target_spellings(dataset, names=names, peers={})
            for relationship in dataset.relationships:
                if (dataset.data_source, relationship.to) not in view.datasets:
                    raise UnknownRelationshipTargetError(dataset=dataset.name, target=relationship.to)
            await self.validate_for_save(dataset, failures=failures)
        merged = self._merged(dataset, view=view)
        try:
            if merged is not dataset:
                merged = SlayerDataset.model_validate(merged.model_dump())
            # Peers the full store cannot load lie outside the view: skipped unreported.
            await self._inner.save_dataset(merged, _validate=_validate, failures=DocumentLoadFailures(report=False))
        except ValueError:  # valid in the view, so a full-store rejection stems from hidden parts
            raise HiddenContentConflictError(kind="dataset", name=dataset.name) from None

    def _merged(self, dataset: SlayerDataset, *, view: _View) -> SlayerDataset:
        key = (dataset.data_source, dataset.name)
        if key in view.hidden:
            raise HiddenContentConflictError(kind="dataset", name=dataset.name)
        stored = view.full.get(key)
        if stored is None:
            if any(n == dataset.name and m.relationships != view.datasets[(ds, n)].relationships for (ds, n), m in view.full.items()):
                raise HiddenContentConflictError(kind="dataset", name=dataset.name)  # a move would drop its pruned relationships
            return dataset
        pruned = [j for j in stored.relationships if (dataset.data_source, j.to) in view.hidden]
        return dataset.model_copy(update={"relationships": [*dataset.relationships, *pruned]}) if pruned else dataset

    async def _save_dataset_impl(self, dataset: SlayerDataset) -> None:
        if self._bypass:
            await self._inner._save_dataset_impl(dataset)
            return
        await self.save_dataset(dataset, _validate=False)

    async def _visible(self, *, data_source: str, name: str) -> bool:
        return (data_source, name) in (await self._view()).datasets

    async def delete_dataset(self, name: str, data_source: str | None = None) -> bool:
        if self._bypass:
            return await self._inner.delete_dataset(name, data_source=data_source)
        target = await self._resolve_target_or_none(name, data_source=data_source)
        if target is None or not await self._visible(data_source=target[0], name=target[1]):
            return False
        return await self._inner.delete_dataset(target[1], data_source=target[0])

    async def _delete_dataset_row(self, *, data_source: str, name: str) -> bool:
        if not self._bypass and not await self._visible(data_source=data_source, name=name):
            return False
        return await self._inner._delete_dataset_row(data_source=data_source, name=name)

    async def update_field_sampled(
        self, *, data_source: str, dataset_name: str, field_name: str,
        sampled: str | None, sampled_values: list[str] | None, distinct_count: int | None,
    ) -> None:
        if not self._bypass and not await self._visible(data_source=data_source, name=dataset_name):
            raise ValueError(f"update_field_sampled: dataset {dataset_name!r} in datasource {data_source!r} not found.")
        await self._inner.update_field_sampled(
            data_source=data_source, dataset_name=dataset_name, field_name=field_name,
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
        """Bypass: the inner delete. Otherwise a hidden prefix is a miss, and one covering hidden rows is refused."""
        if not self._bypass:
            view = await self._view()
            if view.hides(canonical_id_prefix):
                return 0
            if view.hides_under(canonical_id_prefix):
                raise HiddenContentConflictError(kind="embedding", name=canonical_id_prefix)
        return await self._inner.delete_embeddings_for_canonical(canonical_id_prefix=canonical_id_prefix)

