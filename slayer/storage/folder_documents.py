"""Semantic-model documents of a store keeping each datasource's documents in one folder (the stored format): the text
hooks over the folder, and the edits that move datasets between its models."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable, Sequence
from typing import Any

from slayer.core.errors import ModelResolutionError, OssieDocumentInvalidError, UnreadableDocumentError
from slayer.core.granularity import CustomGranularity
from slayer.core.models import SlayerDataset
from slayer.core.semantic_model import OSSIE_VERSION, ossie_validity_errors
from slayer.storage.document_mapping import document_payload, with_dataset, with_document_payload
from slayer.storage.semantic_models import checked_document, decompose
from slayer.storage.stored_layout import (
    MODEL_SUFFIX, Folder, FolderWrite, ModelEdit, Resolved, Workspace, classify, file_name, parsed_shared, referenced_files,
    resolve,
)

__all__ = ["FolderDocuments", "Reading", "document_text", "merged_document", "move_datasets", "replaced_model",
           "replacement", "settle_crossing"]


class Reading:
    """What decomposing a model needs about its datasource."""

    def __init__(self, *, data_source: str, ds_type: str | None, granularities: Sequence[CustomGranularity],
                 reader: str) -> None:
        self.data_source = data_source
        self.ds_type = ds_type
        self.granularities = granularities
        self.reader = reader

    def datasets(self, edit: ModelEdit) -> dict[str, SlayerDataset]:
        """``edit``'s datasets as a load reads them, by case-folded name."""
        if not edit.datasets():
            return {}
        decomposed = decompose(text=document_text(edit.document), name=edit.name, data_source=self.data_source,
                               ds_type=self.ds_type, granularities=self.granularities)
        return {d.name.casefold(): d for d in decomposed.datasets}


def document_text(document: dict[str, Any]) -> str:
    """A resolved document as the text decomposition reads."""
    return json.dumps(document, ensure_ascii=False)


def replacement(folder: Folder, *, name: str, text: str) -> tuple[str, Resolved]:
    """The file model ``name`` stored as ``text`` takes (its own, else a new one) and the model resolved;
    ``OssieDocumentInvalidError`` when it does not resolve to a valid document."""
    _, reasons = parsed_shared(text)
    if reasons:
        raise OssieDocumentInvalidError(name=name, reasons=reasons)
    layout = classify(folder)
    file = layout.models.get(name) or layout.unreadable_files.get(name) or file_name(
        name, suffix=MODEL_SUFFIX, taken=folder.entries())
    trial = folder.model_copy(update={"texts": {**folder.texts, file: text}})
    try:
        resolved = resolve(trial, file)
    except ModelResolutionError as exc:
        raise OssieDocumentInvalidError(name=name, reasons=exc.reasons) from exc
    checked_document(text=document_text(resolved.document), name=name)
    return file, resolved


def replaced_model(folder: Folder, *, name: str, text: str) -> FolderWrite:
    """Model ``name`` stored as ``text``; the dataset documents it no longer lists are deleted."""
    layout = classify(folder)
    file, resolved = replacement(folder, name=name, text=text)
    old: set[str] = set()
    if name in layout.models:
        try:
            old = {r.file for r in resolve(folder, file).refs.values()}
        except ModelResolutionError:
            old = set()
    kept = {r.file for r in resolved.refs.values()}
    others = set()
    for other, other_file in layout.models.items():
        if other != name:
            data, _ = parsed_shared(folder.texts[other_file])
            others |= referenced_files(data)
    return FolderWrite(model_writes={file: text}, deletes=sorted(old - kept - others))


class FolderDocuments:
    """The semantic-model text hooks of a store whose datasource folders hold its documents."""

    async def _folder_data_sources(self) -> list[str]:
        """Every datasource with stored documents."""
        raise NotImplementedError

    async def _read_folder(self, data_source: str) -> Folder:
        """``data_source``'s documents as a load reads them (a pending journal read as completed)."""
        raise NotImplementedError

    async def _write_folder(self, data_source: str, plan: Callable[[Folder], FolderWrite], *,
                            legacy: Sequence[str] = ()) -> None:
        """Apply ``plan(folder)`` under the datasource's write serialisation, after completing any pending journal;
        ``folder.legacy`` holds the raw stored dicts of the 2.0 standalone datasets ``legacy``, read under it."""
        raise NotImplementedError

    async def _list_semantic_model_identities(self) -> list[tuple[str, str]]:
        out: list[tuple[str, str]] = []
        for ds in await self._folder_data_sources():
            layout = classify(await self._read_folder(ds))
            out += [(ds, n) for n in sorted({*layout.models, *layout.unreadable})]
        return out

    async def _get_semantic_model_text_impl(self, *, data_source: str, name: str) -> str | None:
        folder = await self._read_folder(data_source)
        layout = classify(folder)
        file = layout.models.get(name)
        if file is None and name in layout.unreadable:
            text = folder.texts.get(layout.unreadable_files.get(name, ""))
            if text is None:
                raise UnreadableDocumentError(reasons=[layout.unreadable[name]], name=name)
            return text
        return folder.texts[file] if file is not None else None

    async def _save_semantic_model_text_impl(self, *, data_source: str, name: str, text: str) -> None:
        await self._write_folder(data_source, lambda folder: replaced_model(folder, name=name, text=text))

    async def _delete_semantic_model_row(self, *, data_source: str, name: str) -> bool:
        deleted: list[str] = []

        def plan(folder: Folder) -> FolderWrite:
            ws = Workspace(folder)
            if name not in ws.layout.models and name not in ws.layout.unreadable:
                return FolderWrite()
            ws.delete(name)
            deleted.append(name)
            return ws.plan()

        await self._write_folder(data_source, plan)
        return bool(deleted)

    async def _patch_semantic_model_text(self, *, data_source: str, name: str, patch: Callable[[str], str]) -> str:
        written: list[str] = []

        def plan(folder: Folder) -> FolderWrite:
            file = classify(folder).models.get(name)
            if file is None:
                raise ValueError(f"semantic model {name!r} of datasource {data_source!r} not found")
            written.append(patch(folder.texts[file]))
            return FolderWrite(model_writes={file: written[-1]})

        await self._write_folder(data_source, plan)
        return written[0]


# --------------------------------------------------------------------------- #
# Moves
# --------------------------------------------------------------------------- #
def _fold(name: Any) -> str:
    return str(name).casefold()


def merged_document(stored: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    """``stored`` with ``incoming``'s datasets and relationships (each replacing a same-named one) and its model-level
    ``SLAYER`` relationships added."""
    out = copy.deepcopy(stored)
    for key in ("datasets", "relationships"):
        names = {_fold(e.get("name")) for e in incoming.get(key) or []}
        kept = [e for e in out.get(key) or [] if _fold(e.get("name")) not in names]
        merged = [*kept, *copy.deepcopy(incoming.get(key) or [])]
        if merged:
            out[key] = merged
    crossing = document_payload(incoming).relationships
    if crossing:
        payload = document_payload(out)
        payload.relationships = [*payload.relationships, *crossing]
        with_document_payload(out, payload)
    return out


def settle_crossing(edit: ModelEdit, reading: Reading) -> None:
    """Rewrite each relationship ``edit``'s ``SLAYER`` entry holds whose target the model now holds as core, on the
    dataset declaring it."""
    members = {_fold(d.get("name")) for d in edit.datasets()}
    owners = {_fold(d.dataset) for d in document_payload(edit.document).relationships
              if _fold(d.relationship.to) in members and _fold(d.dataset) in members}
    for owner in sorted(owners):
        current = reading.datasets(edit)[owner]
        edit.document = with_dataset(edit.document, old=current.model_copy(update={"relationships": []}),
                                     new=current, reader=reading.reader)


def strip_relationships(edit: ModelEdit, moved: set[str]) -> None:
    """Remove ``edit``'s relationships touching a dataset of ``moved``, core and crossing."""
    rels = [r for r in edit.document.get("relationships") or []
            if _fold(r.get("from")) not in moved and _fold(r.get("to")) not in moved]
    if rels:
        edit.document["relationships"] = rels
    else:
        edit.document.pop("relationships", None)
    payload = document_payload(edit.document)
    kept = [d for d in payload.relationships if _fold(d.dataset) not in moved and _fold(d.relationship.to) not in moved]
    if kept != payload.relationships:
        payload.relationships = kept
        with_document_payload(edit.document, payload)


def move_datasets(ws: Workspace, *, target: str, names: Sequence[str], legacy: dict[str, SlayerDataset],
                  reading: Reading, create: bool) -> None:
    """Move datasets ``names`` of any model of the datasource (or 2.0 standalone ones, ``legacy`` by case-folded name)
    into model ``target`` (created, split, when absent and ``create``), with the relationships they take part in. A
    moved dataset keeps its stored text; each relationship is rewritten on the model holding its ``from`` dataset."""
    dst = ws.model(target)
    if dst is None:
        if not create:
            raise ValueError(f"Semantic model {target!r} not found in datasource {reading.data_source!r}")
        dst = ws.create(target, version=OSSIE_VERSION, split=True)
    sources: dict[str, str] = {}
    for name in names:
        owner = ws.owner(name)
        if owner is None and _fold(name) not in legacy:
            raise ValueError(f"Dataset {name!r} not found in datasource {reading.data_source!r}")
        if owner != target and owner is not None:
            sources[_fold(name)] = owner
    moved = set(sources) | {f for f in legacy if f in {_fold(n) for n in names}}
    involved = {target, *sources.values()}
    edits = {m: ws.model(m) for m in involved}
    views = {m: reading.datasets(e) for m, e in edits.items() if e is not None}
    touched: list[tuple[str, SlayerDataset]] = [
        (_fold(d.name), d) for view in views.values() for d in view.values()
        if _fold(d.name) in moved or any(_fold(r.to) in moved for r in d.relationships)]
    touched += [(folded, legacy[folded]) for folded in moved if folded in legacy]
    for edit in edits.values():
        if edit is not None:
            strip_relationships(edit, moved)
    for folded, owner in sources.items():
        src = edits[owner]
        assert src is not None
        entry, ref, inline = src.take(folded)
        dst.give(entry, ref=ref, inline=inline)
    for folded, dataset in legacy.items():
        if folded in moved:
            dst.document = with_dataset(dst.document, old=None, new=dataset.model_copy(update={"relationships": []}),
                                        reader=reading.reader)
    for folded, original in touched:
        if not original.relationships:
            continue
        holder = dst if folded in moved else next(e for e in edits.values() if e is not None and e.dataset(folded))
        current = reading.datasets(holder)[folded]
        holder.document = with_dataset(holder.document, old=current,
                                       new=current.model_copy(update={"relationships": list(original.relationships)}),
                                       reader=reading.reader)
    for edit in edits.values():
        if edit is not None and edit.datasets():
            reasons = ossie_validity_errors(edit.document)
            if reasons:
                raise OssieDocumentInvalidError(name=edit.name, reasons=reasons)


def stored_entities(folder: Folder) -> dict[str, dict[str, Any]]:
    """Every dataset entity the folder's resolvable models hold, by name."""
    out: dict[str, dict[str, Any]] = {}
    layout = classify(folder)
    for file in layout.models.values():
        try:
            document = resolve(folder, file).document
        except ModelResolutionError:
            continue
        out.update({d["name"]: d for d in document.get("datasets") or [] if isinstance(d, dict) and "name" in d})
    return out

