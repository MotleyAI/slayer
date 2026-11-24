"""Persist stored documents in the current format: ``slayer storage upgrade`` over a store, and ``upgrade_datasets``
over documents SLayer does not own. Both run the upgrade every load applies in memory."""

from __future__ import annotations

import copy
import difflib
import warnings
from collections.abc import Awaitable, Mapping, Sequence
from typing import Any, Final, Literal, NoReturn

import yaml
from pydantic import BaseModel

from slayer.async_utils import run_sync
from slayer.core.errors import OssieDocumentInvalidError, UnreadableDocumentError
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.warnings import SlayerStoredUpgradeNeededWarning
from slayer.embeddings.models import Embedding
from slayer.memories.models import Memory
from slayer.storage.base import (
    CLIENT_SOURCE, DATASOURCE_TYPES, ClientSource, LiveRefinementRequiredError, StorageBackend, _memory_in_datasource,
    one_upgrade_pass, without_none,
)
from slayer.storage.document_loading import Loaded
from slayer.storage.semantic_models import checked_document

DocumentKind = Literal["dataset", "semantic model", "memory", "datasource"]
Status = Literal["upgraded", "current", "would upgrade", "not upgraded", "conflict"]
WOULD_UPGRADE: Final = "would upgrade"
NOT_UPGRADED: Final = "not upgraded"
SEMANTIC_MODEL: Final = "semantic model"


class Refusal(BaseModel):
    """A document the upgrade cannot complete, and why."""

    name: str
    reason: str


class UpgradedDatasets(BaseModel):
    """``upgrade_datasets``' result: the completed documents in the current format, and the refused ones."""

    documents: list[dict[str, Any]]
    refused: list[Refusal]


class DocumentOutcome(BaseModel):
    kind: DocumentKind
    name: str
    status: Status
    detail: str | None = None

    def line(self) -> str:
        return f"{self.kind} {self.name}: {self.status}" + (f": {self.detail}" if self.detail else "")


class StoreUpgradeReport(BaseModel):
    outcomes: list[DocumentOutcome]

    @property
    def complete(self) -> bool:
        return all(o.status in ("upgraded", "current", WOULD_UPGRADE) for o in self.outcomes)


class _Plan(BaseModel):
    """One stored document: its raw form as read, and its upgraded raw form when the upgrade changes it."""

    kind: Literal["dataset", "memory", "datasource"]
    data_source: str | None
    name: str
    shown: str
    raw: dict[str, Any]
    upgraded: dict[str, Any] | None = None
    refused: str | None = None


def _dialect_refusal(data_source: str) -> str:
    return (f"its field sql waits for datasource {data_source!r}'s dialect; store the datasource or pass its type "
            f"(--datasource-type {data_source}=TYPE)")


async def _planned(plan: _Plan, upgraded: Awaitable[tuple[BaseModel, bool]]) -> _Plan:
    """``plan`` with its upgrade's outcome: refused on any failure, so the other documents go on."""
    try:
        document, changes = await upgraded
    except Exception as exc:  # noqa: BLE001
        return plan.model_copy(update={"refused": str(exc)})
    if isinstance(document, SlayerDataset) and any(f.sql is not None for f in document.fields):
        return plan.model_copy(update={"refused": _dialect_refusal(str(plan.data_source))})
    return plan.model_copy(update={"upgraded": without_none(document.model_dump(mode="json"))}) if changes else plan


async def _dataset_plan(storage: StorageBackend, *, data_source: str, name: str, raw: dict[str, Any]) -> _Plan:
    plan = _Plan(kind="dataset", data_source=data_source, name=name, shown=f"{data_source}.{name}", raw=raw)
    return await _planned(plan, storage._upgraded_dataset(name=name, data=copy.deepcopy(raw), data_source=data_source))


async def _memory_plan(storage: StorageBackend, *, memory_id: str, raw: dict[str, Any]) -> _Plan:
    plan = _Plan(kind="memory", data_source=None, name=memory_id, shown=memory_id, raw=raw)
    return await _planned(plan, storage._upgraded_memory(memory_id=memory_id, data=copy.deepcopy(raw)))


async def _datasource_plan(storage: StorageBackend, *, name: str, raw: dict[str, Any]) -> _Plan:
    plan = _Plan(kind="datasource", data_source=None, name=name, shown=name, raw=raw)
    return await _planned(plan, storage._upgraded_datasource(name=name, data=copy.deepcopy(raw)))


async def _semantic_model_outcome(storage: StorageBackend, *, data_source: str, name: str) -> DocumentOutcome:
    """A semantic-model document has no upgrade: it is current when valid, else reported with its reasons."""
    shown = f"{data_source}.{name}"
    try:
        text = await storage.get_semantic_model_text(name, data_source=data_source)
        checked_document(text=text or "", name=name)
    except (UnreadableDocumentError, OssieDocumentInvalidError) as exc:
        return DocumentOutcome(kind=SEMANTIC_MODEL, name=shown, status=NOT_UPGRADED, detail=str(exc))
    return DocumentOutcome(kind=SEMANTIC_MODEL, name=shown, status="current")


async def _unreadable(load: Awaitable[Any]) -> str:
    """Why a listed document has no readable stored dict: its load error."""
    try:
        await load
    except Exception as exc:  # noqa: BLE001
        return str(exc)
    return "its stored document cannot be read"


async def _unreadable_outcomes(storage: StorageBackend, *, data_source: str | None, memories: Mapping[str, Any],
                               datasources: Mapping[str, Any]) -> list[DocumentOutcome]:
    """Each listed memory and datasource without a readable stored dict that fails to load; a memory only on a
    whole-store run, since an unreadable one names no datasource."""
    out: list[DocumentOutcome] = []
    for name in await storage.list_datasources():
        if data_source not in (None, name) or name in datasources:
            continue
        try:  # a backend without raw access lists datasources that load
            await storage.get_datasource(name)
        except Exception as exc:  # noqa: BLE001
            out.append(DocumentOutcome(kind="datasource", name=name, status=NOT_UPGRADED, detail=str(exc)))
    if data_source is None:
        _, failures = await storage._list_memories_rows(entities=None)
        out += [DocumentOutcome(kind="memory", name=f.name, status=NOT_UPGRADED, detail=f.cause_text)
                for f in failures if f.name not in memories]
    return out


def _diff(plan: _Plan) -> str:
    def text(data: dict[str, Any]) -> list[str]:
        return yaml.safe_dump(data, sort_keys=False, allow_unicode=True).splitlines(keepends=True)

    assert plan.upgraded is not None
    return "".join(difflib.unified_diff(text(plan.raw), text(plan.upgraded), fromfile=f"{plan.shown} (stored)",
                                        tofile=f"{plan.shown} (upgraded)"))


async def _plans(storage: StorageBackend, *, data_source: str | None) -> tuple[list[_Plan], list[DocumentOutcome]]:
    """Every in-scope document read first, then upgraded in memory; the unreadable ones as outcomes."""
    identities = await storage._list_all_dataset_identities()
    # A semantic-model document's datasets are its own; the document is reported whole.
    datasets = [(d, n) for d, n in identities
                if data_source in (None, d) and await storage._owner_of(n, data_source=d) is None]
    raws = {(d, n): await storage._load_raw_dataset_dict(name=n, data_source=d) for d, n in datasets}
    memories = {m: raw for m, raw in (await storage._raw_memory_dicts()).items()
                if data_source is None or _memory_in_datasource(raw, data_source=data_source, identities=identities)}
    datasources = {n: raw for n, raw in (await storage._raw_datasource_dicts()).items() if data_source in (None, n)}
    plans: list[_Plan] = []
    unreadable: list[DocumentOutcome] = []
    for (d, n), raw in raws.items():
        if raw is None:
            unreadable.append(DocumentOutcome(kind="dataset", name=f"{d}.{n}", status=NOT_UPGRADED,
                                              detail=await _unreadable(storage.get_dataset(n, data_source=d))))
        else:
            plans.append(await _dataset_plan(storage, data_source=d, name=n, raw=raw))
    plans += [await _memory_plan(storage, memory_id=m, raw=raw) for m, raw in memories.items()]
    plans += [await _datasource_plan(storage, name=n, raw=raw) for n, raw in datasources.items()]
    unreadable += await _unreadable_outcomes(storage, data_source=data_source, memories=memories,
                                             datasources=datasources)
    return plans, unreadable


async def _outcome(storage: StorageBackend, plan: _Plan, *, dry_run: bool) -> DocumentOutcome:
    if plan.refused is not None:
        return DocumentOutcome(kind=plan.kind, name=plan.shown, status=NOT_UPGRADED, detail=plan.refused)
    if plan.upgraded is None:
        return DocumentOutcome(kind=plan.kind, name=plan.shown, status="current")
    if dry_run:
        return DocumentOutcome(kind=plan.kind, name=plan.shown, status=WOULD_UPGRADE, detail="\n" + _diff(plan))
    written = await storage._write_raw_if_unchanged(kind=plan.kind, data_source=plan.data_source, name=plan.name,
                                                    expected=plan.raw, data=plan.upgraded)
    if not written:
        return DocumentOutcome(kind=plan.kind, name=plan.shown, status="conflict",
                               detail="changed in the store since the upgrade read it; left as it is")
    return DocumentOutcome(kind=plan.kind, name=plan.shown, status="upgraded")


async def _held_by(storage: StorageBackend, *, datasource: str, plans: Sequence[_Plan],
                   outcomes: Sequence[DocumentOutcome]) -> list[str]:
    """The dependents not stored in the current format that a renaming ``datasource`` must wait for: its datasets,
    the memories reading it, and every unreadable memory (which names no datasource), whatever the run's scope."""
    if datasource not in await storage._granularity_renames():
        return []
    identities = await storage._list_all_dataset_identities()
    memories = {p.shown: p.raw for p in plans if p.kind == "memory"}

    def depends(outcome: DocumentOutcome) -> bool:
        if outcome.kind == "dataset":
            return outcome.name.startswith(f"{datasource}.")
        raw = memories.get(outcome.name)
        return raw is not None and _memory_in_datasource(raw, data_source=datasource, identities=identities)

    held = [f"{o.kind} {o.name}" for o in outcomes
            if o.kind in ("dataset", "memory") and o.status in (NOT_UPGRADED, "conflict") and depends(o)]
    readable = await storage._raw_memory_dicts()
    _, failures = await storage._list_memories_rows(entities=None)
    return held + [f"memory {f.name}" for f in failures if f.name not in readable]


async def upgrade_store(
    storage: StorageBackend, *, data_source: str | None = None, dry_run: bool = False,
    datasource_types: Mapping[str, str] | None = None, client_source: ClientSource | None = None,
) -> StoreUpgradeReport:
    """Store every in-scope document the upgrade changes in the current format (dependents before their datasource),
    each only if unchanged since read; ``client_source`` refines documents below v8 against their live datasource."""
    types_token = DATASOURCE_TYPES.set(dict(datasource_types or {}))
    client_token = CLIENT_SOURCE.set(client_source)
    try:
        with warnings.catch_warnings(), one_upgrade_pass():
            warnings.simplefilter("ignore", SlayerStoredUpgradeNeededWarning)
            plans, outcomes = await _plans(storage, data_source=data_source)
            for plan in [p for p in plans if p.kind != "datasource"]:
                outcomes.append(await _outcome(storage, plan, dry_run=dry_run))
            for plan in [p for p in plans if p.kind == "datasource"]:
                held = await _held_by(storage, datasource=plan.name, plans=plans, outcomes=outcomes)
                outcomes.append(DocumentOutcome(
                    kind="datasource", name=plan.shown, status=NOT_UPGRADED,
                    detail=f"its granularity rename waits for {', '.join(held)}") if held
                    else await _outcome(storage, plan, dry_run=dry_run))
            outcomes += [await _semantic_model_outcome(storage, data_source=d, name=n)
                         for d, n in await storage.list_semantic_models(data_source=data_source)]
    finally:
        CLIENT_SOURCE.reset(client_token)
        DATASOURCE_TYPES.reset(types_token)
    return StoreUpgradeReport(outcomes=outcomes)


def _read_only(what: str) -> NoReturn:
    raise RuntimeError(f"upgrade_datasets writes nothing: {what}")


class _Snapshot(StorageBackend):
    """One datasource's stored dataset documents, read as a store holding only them; every write refused."""

    def __init__(self, *, documents: Mapping[str, dict[str, Any]], data_source: str,
                 datasource: DatasourceConfig | None) -> None:
        self._documents = documents
        self._data_source = data_source
        self._datasource = datasource

    async def _load_raw_dataset_dict(self, *, name: str, data_source: str) -> dict | None:
        raw = self._documents.get(name) if data_source == self._data_source else None
        return copy.deepcopy(raw)

    async def _raw_datasource_dicts(self) -> dict[str, dict]:
        datasource = self._datasource
        return {datasource.name: without_none(datasource.model_dump(mode="json"))} if datasource is not None else {}

    async def _list_all_dataset_identities(self) -> list[tuple[str, str]]:
        return [(self._data_source, name) for name in self._documents]

    async def get_dataset(self, name: str, data_source: str | None = None) -> SlayerDataset | None:
        raw = await self._load_raw_dataset_dict(name=name, data_source=data_source or self._data_source)
        if raw is None:
            return None
        return (await self._upgraded_dataset(name=name, data=raw, data_source=self._data_source))[0]

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
        return self._datasource if self._datasource is not None and self._datasource.name == name else None

    async def list_datasources(self) -> list[str]:
        return [self._datasource.name] if self._datasource is not None else []

    async def get_datasource_priority(self) -> list[str]:
        return []

    async def _save_dataset_impl(self, dataset: SlayerDataset) -> None:
        _read_only("saving a dataset")

    async def _delete_dataset_row(self, *, data_source: str, name: str) -> bool:
        _read_only("deleting a dataset")

    async def update_field_sampled(self, **_: Any) -> None:
        _read_only("recording a field sample")

    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        _read_only("saving a datasource")

    async def _delete_datasource_row(self, name: str) -> bool:
        _read_only("deleting a datasource")

    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        _read_only("setting the datasource priority")

    async def _save_memory_row(self, memory: Memory) -> None:
        _read_only("saving a memory")

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        return None

    async def _list_memories_rows(self, *, entities: list[str] | None) -> Loaded[Memory]:
        return [], []

    async def _delete_memory_row(self, memory_id: str) -> bool:
        _read_only("deleting a memory")

    async def _next_memory_seq(self) -> str:
        _read_only("allocating a memory id")

    async def save_embedding(self, row: Embedding) -> None:
        _read_only("saving an embedding")

    async def get_embedding(self, *, canonical_id: str, embedding_model_name: str) -> Embedding | None:
        return None

    async def list_embeddings(self, *, embedding_model_name: str) -> list[Embedding]:
        return []

    async def delete_embeddings_for_canonical(self, *, canonical_id_prefix: str) -> int:
        _read_only("deleting embeddings")


def _data_source_of(documents: Sequence[dict[str, Any]], datasource: DatasourceConfig | None) -> str:
    if datasource is not None:
        return datasource.name
    return next((d["data_source"] for d in documents if isinstance(d.get("data_source"), str)), "default")


async def _upgrade_datasets(snapshot: _Snapshot, documents: Sequence[dict[str, Any]],
                            data_source: str) -> UpgradedDatasets:
    out: list[dict[str, Any]] = []
    refused: list[Refusal] = []
    for document in documents:
        name = str(document.get("name"))
        try:
            dataset, _ = await snapshot._upgraded_dataset(name=name, data=copy.deepcopy(document),
                                                          data_source=data_source)
        except LiveRefinementRequiredError:
            refused.append(Refusal(name=name, reason=(
                f"written below version 8, it needs live type refinement against datasource {data_source!r}; "
                f"run `slayer storage upgrade` on a store holding it")))
            continue
        if any(f.sql is not None for f in dataset.fields):
            refused.append(Refusal(name=name, reason=(
                f"its field sql waits for datasource {data_source!r}'s dialect; pass datasource_type or datasource")))
            continue
        out.append(without_none(dataset.model_dump(mode="json")))
    return UpgradedDatasets(documents=out, refused=refused)


def upgrade_datasets(
    *, documents: Sequence[dict[str, Any]], datasource_type: str | None = None,
    datasource: DatasourceConfig | None = None,
) -> UpgradedDatasets:
    """One datasource's stored dataset documents in the current format, as ``slayer storage upgrade`` would store
    them; opens no store and writes nothing."""
    data_source = _data_source_of(documents, datasource)
    snapshot = _Snapshot(documents={str(d.get("name")): d for d in documents}, data_source=data_source,
                         datasource=datasource)
    types_token = DATASOURCE_TYPES.set({data_source: datasource_type} if datasource_type else {})
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SlayerStoredUpgradeNeededWarning)
            return run_sync(_upgrade_datasets(snapshot, documents, data_source))
    finally:
        DATASOURCE_TYPES.reset(types_token)
