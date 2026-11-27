"""A datasource's stored documents as Ossie multi-file models: model documents (flat, or split with ``dataset_refs``)
and dataset documents in one folder, recognised by content.

Pure: backends read a :class:`Folder` and apply the :class:`FolderWrite` a :class:`Workspace` plans.
"""

from __future__ import annotations

import copy
import functools
import hashlib
import json
import re
from collections.abc import Iterable
from typing import Any
from urllib.parse import quote, unquote

import yaml
from pydantic import BaseModel

from slayer.core.document_text import edit_text, parse_document_text
from slayer.core.errors import ModelResolutionError, UnreadableDocumentError

__all__ = ["DATASET_SUFFIX", "JOURNAL", "MODEL_SUFFIX", "Folder", "FolderWrite", "Layout", "ModelEdit", "Ref",
           "Resolved", "Workspace", "applied", "classify", "dump", "file_name", "file_of", "iri_of", "journal_text",
           "parsed", "pending_journal", "readable_stem", "referenced_files", "resolve"]

MODEL_SUFFIX = ".semantic_model.yaml"
DATASET_SUFFIX = ".dataset.yaml"
JOURNAL = ".journal.json"
DOCUMENT_SUFFIXES = (".yaml", ".yml", ".json")
_STEM_BYTES = 120
_DEVICES = frozenset({"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(1, 10)),
                      *(f"LPT{i}" for i in range(1, 10))})
_PLAIN = frozenset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-")
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")


# --------------------------------------------------------------------------- #
# File names
# --------------------------------------------------------------------------- #
def readable_stem(name: str) -> str:
    """``name`` when made of ASCII letters, digits, ``_`` and ``-``; else with every other UTF-8 byte percent-encoded."""
    return "".join(chr(b) if b in _PLAIN else f"%{b:02X}" for b in name.encode("utf-8"))


def _hashed(name: str, stem: str) -> str:
    suffix = "-" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:12]
    cut = stem[:_STEM_BYTES - len(suffix)]
    escape = cut.rfind("%")
    if escape >= 0 and len(cut) - escape < 3:
        cut = cut[:escape]
    return cut + suffix


def file_name(name: str, *, suffix: str, taken: Iterable[str]) -> str:
    """A new file name for entity ``name``: its readable stem, cut and hashed when too long, a device name, or equal
    (ignoring case) to a ``taken`` entry."""
    stem = readable_stem(name)
    folded = {t.casefold() for t in taken}
    if len(stem) > _STEM_BYTES or stem.upper() in _DEVICES or f"{stem}{suffix}".casefold() in folded:
        stem = _hashed(name, stem)
    return stem + suffix


def iri_of(file: str) -> str:
    """``file`` as a ``dataset_refs`` ``iri``: one percent-encoded path segment."""
    return quote(file, safe="")


def file_of(iri: Any) -> str:
    """The file name an ``iri`` names in its model's folder; ``ValueError`` (the reason) when it breaks the same-folder
    rule."""
    if not isinstance(iri, str) or not iri:
        raise ValueError("is not a non-empty string")
    if "/" in iri or "\\" in iri:
        raise ValueError("is not one path segment of the folder")
    if _SCHEME.match(iri) or "?" in iri or "#" in iri:
        raise ValueError("is not a relative reference (it has a scheme, query or fragment)")
    if _ESCAPE.search(iri):
        raise ValueError("has a % not followed by two hexadecimal digits")
    try:
        decoded = unquote(iri, errors="strict")
    except UnicodeDecodeError as exc:
        raise ValueError("does not percent-decode to UTF-8 text") from exc
    if decoded in ("", ".", "..") or any(c in decoded for c in "\0/\\"):
        raise ValueError("does not name a file of the folder")
    return decoded


def stem_name(file: str) -> str:
    """The entity name a file name spells, for a document whose content declares none."""
    stem = file.removesuffix(MODEL_SUFFIX) if file.endswith(MODEL_SUFFIX) else file.rsplit(".", 1)[0]
    return unquote(stem, errors="replace")


def dump(data: Any) -> str:
    """A new stored document's text."""
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=1 << 16)


# --------------------------------------------------------------------------- #
# Reading
# --------------------------------------------------------------------------- #
class Folder(BaseModel):
    """One datasource's stored documents as read: each entry's text, or why it cannot be read."""

    data_source: str
    texts: dict[str, str] = {}
    problems: dict[str, str] = {}
    #: Legacy standalone datasets a pending journal retires (read as gone).
    retired: list[str] = []
    #: Whether a pending journal was applied in memory.
    pending: bool = False
    #: Entries read from a 2.0 location (SQLite's ``semantic_models`` table); writing one moves it.
    legacy_files: list[str] = []
    #: The raw stored dicts of the 2.0 standalone datasets a write asked for, read under its serialisation.
    legacy: dict[str, Any] = {}

    def entries(self) -> set[str]:
        return set(self.texts) | set(self.problems)


@functools.lru_cache(maxsize=2048)
def _parse(text: str) -> tuple[Any, tuple[str, ...]]:
    try:
        return parse_document_text(text), ()
    except UnreadableDocumentError as exc:
        return None, tuple(exc.reasons)


def parsed(text: str) -> tuple[Any, list[str]]:
    """``text`` parsed (a private copy) and the reasons it does not read, if so."""
    data, reasons = _parse(text)
    return copy.deepcopy(data), list(reasons)


def parsed_shared(text: str) -> tuple[Any, list[str]]:
    """:func:`parsed` without the copy: the parse every reader shares, never to be mutated."""
    data, reasons = _parse(text)
    return data, list(reasons)


def _is_document_file(file: str) -> bool:
    return file.endswith(DOCUMENT_SUFFIXES)


class Layout(BaseModel):
    """A folder's documents by kind."""

    #: Model name → its file (the first, when several declare one name).
    models: dict[str, str] = {}
    #: Model name → every file declaring it, when more than one does.
    clashes: dict[str, list[str]] = {}
    #: Dataset-document file → the dataset name it declares.
    datasets: dict[str, str] = {}
    #: Model name → why its file does not read.
    unreadable: dict[str, str] = {}
    #: Model name → its file, for a model whose file does not read.
    unreadable_files: dict[str, str] = {}
    #: Files the model documents reference.
    referenced: set[str] = set()


def referenced_files(data: Any) -> set[str]:
    refs = data.get("dataset_refs") if isinstance(data, dict) else None
    out: set[str] = set()
    for ref in refs if isinstance(refs, list) else []:
        try:
            out.add(file_of(ref.get("iri") if isinstance(ref, dict) else None))
        except ValueError:
            continue
    return out


def classify(folder: Folder) -> Layout:
    """``folder``'s model and dataset documents; a file a model references is never a model itself, and a document
    file that does not read is an unloadable model unless it is named like a dataset document."""
    layout = Layout()
    unread: dict[str, str] = {}
    candidates: list[tuple[str, Any]] = []
    for file in sorted(folder.entries()):
        problem = folder.problems.get(file)
        data, reasons = (None, [problem]) if problem is not None else parsed_shared(folder.texts[file])
        if reasons:
            if _is_document_file(file) and not file.endswith(DATASET_SUFFIX):
                unread[file] = "; ".join(r for r in reasons if r)
            continue
        if isinstance(data, dict) and "dataset" in data:
            entry = data.get("dataset")
            layout.datasets[file] = str(entry.get("name")) if isinstance(entry, dict) else stem_name(file)
        elif _is_document_file(file):
            candidates.append((file, data))
            layout.referenced |= referenced_files(data)
    for file, data in candidates:
        if file in layout.referenced:
            continue
        name = data.get("name") if isinstance(data, dict) else None
        name = name if isinstance(name, str) and name else stem_name(file)
        if name in layout.models:
            layout.clashes.setdefault(name, [layout.models[name]]).append(file)
            continue
        layout.models[name] = file
    for file, reason in unread.items():
        name = stem_name(file)
        if file not in layout.referenced and name not in layout.models and name not in layout.unreadable:
            layout.unreadable[name] = reason
            layout.unreadable_files[name] = file
    return layout


# --------------------------------------------------------------------------- #
# Resolution
# --------------------------------------------------------------------------- #
class Ref(BaseModel):
    """A ``dataset_refs`` entry and the file it names."""

    name: str | None = None
    iri: str
    file: str


class Resolved(BaseModel):
    """A model document resolved: the flat document every consumer reads, and where each dataset is stored."""

    name: str
    file: str
    #: The stored model document, parsed.
    stored: Any
    document: dict[str, Any]
    #: Case-folded names of the datasets declared inline.
    inline: list[str] = []
    #: Case-folded dataset name → its reference.
    refs: dict[str, Ref] = {}
    split: bool = False


def _entry_problem(folder: Folder, file: str) -> str | None:
    if file in folder.problems:
        return folder.problems[file]
    if file in folder.texts:
        return None
    twin = next((e for e in sorted(folder.entries()) if e.casefold() == file.casefold()), None)
    if twin is not None:
        return f"differs in case from the folder's entry {twin!r}"
    return "does not exist in the folder"


def _names(entries: Any) -> list[str]:
    return [str(d.get("name")) for d in entries if isinstance(d, dict)] if isinstance(entries, list) else []


def _referenced(folder: Folder, *, data: dict[str, Any], reasons: list[str], involved: list[str],
                held: dict[str, list[str]]) -> list[tuple[Ref, dict[str, Any]]]:
    """Each ``dataset_refs`` entry's reference and dataset; every problem appended to ``reasons``."""
    raw = data.get("dataset_refs")
    if not isinstance(raw, list) or not raw:
        reasons.append("its dataset_refs is not a non-empty list")
        return []
    out: list[tuple[Ref, dict[str, Any]]] = []
    files: set[str] = set()
    for i, ref in enumerate(raw, start=1):
        ok = isinstance(ref, dict) and not set(ref) - {"name", "iri"} and (
            "name" not in ref or (isinstance(ref["name"], str) and ref["name"]))
        if not ok:
            reasons.append(f"dataset_refs entry {i} is not a reference (a name and an iri)")
            continue
        iri = ref.get("iri")
        try:
            target = file_of(iri)
        except ValueError as exc:
            involved.append(str(iri))
            reasons.append(f"the reference {iri!r} {exc}")
            continue
        involved.append(target)
        problem = _entry_problem(folder, target)
        if problem is not None:
            reasons.append(f"{target!r} (reference {iri!r}) {problem}")
            continue
        if target in files:
            reasons.append(f"{target!r} is referenced twice")
            continue
        files.add(target)
        doc, errors = parsed_shared(folder.texts[target])
        if errors:
            reasons.append(f"{target!r} is not YAML or JSON text: {'; '.join(errors)}")
            continue
        if not isinstance(doc, dict) or set(doc) != {"version", "dataset"} or not isinstance(doc["dataset"], dict):
            reasons.append(f"{target!r} is not a dataset document (exactly version and dataset)")
            continue
        if doc["version"] != data.get("version"):
            reasons.append(f"{target!r} has version {doc['version']!r}, its model {data.get('version')!r}")
            continue
        declared = doc["dataset"].get("name")
        if "name" in ref and declared != ref["name"]:
            reasons.append(f"{target!r} holds dataset {declared!r}, where its reference names {ref['name']!r}")
            continue
        held.setdefault(str(declared).casefold(), []).append(target)
        out.append((Ref(name=ref.get("name"), iri=str(iri), file=target), copy.deepcopy(doc["dataset"])))
    return out


def resolve(folder: Folder, file: str) -> Resolved:
    """Model document ``file`` of ``folder`` resolved; ``ModelResolutionError`` naming every file involved when its
    references do not resolve."""
    data, _ = parsed_shared(folder.texts[file])
    declared = data.get("name") if isinstance(data, dict) else None
    name = declared if isinstance(declared, str) else stem_name(file)
    if not isinstance(data, dict) or "dataset_refs" not in data:
        inline = [n.casefold() for n in _names(data.get("datasets") if isinstance(data, dict) else None)]
        return Resolved(name=name, file=file, stored=data, document=copy.deepcopy(data) if isinstance(data, dict)
                        else {}, inline=inline)
    reasons: list[str] = []
    involved = [file]
    raw_inline = data.get("datasets")
    inline_entries: list[Any] = raw_inline if isinstance(raw_inline, list) else []
    held: dict[str, list[str]] = {n.casefold(): [file] for n in _names(inline_entries)}
    referenced = _referenced(folder, data=data, reasons=reasons, involved=involved, held=held)
    for folded, holders in held.items():
        if len(holders) > 1:
            reasons.append(f"dataset {folded!r} is held more than once ({', '.join(map(repr, holders))})")
    if reasons:
        raise ModelResolutionError(model=name, files=list(dict.fromkeys(involved)),
                                   reasons=[f"{file}: {r}" for r in reasons])
    datasets = [*copy.deepcopy(inline_entries), *(d for _, d in referenced)]
    document: dict[str, Any] = {}
    for key, value in data.items():
        if key == "datasets" or (key == "dataset_refs" and "datasets" not in data):
            document["datasets"] = datasets
        elif key != "dataset_refs":
            document[key] = copy.deepcopy(value)
    return Resolved(name=name, file=file, stored=data, document=document,
                    inline=[n.casefold() for n in _names(inline_entries)],
                    refs={str(d.get("name")).casefold(): r for r, d in referenced}, split=True)


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #
class FolderWrite(BaseModel):
    """The file changes of one write to a folder, in order: dataset documents, model documents, deletions, then the
    retired legacy standalone datasets."""

    writes: dict[str, str] = {}
    model_writes: dict[str, str] = {}
    deletes: list[str] = []
    retired: list[str] = []

    def ordered_writes(self) -> list[tuple[str, str]]:
        return [*self.writes.items(), *self.model_writes.items()]

    def steps(self) -> int:
        return len(self.writes) + len(self.model_writes) + len(self.deletes) + len(self.retired)

    def empty(self) -> bool:
        return self.steps() == 0


def journal_text(write: FolderWrite) -> str:
    return json.dumps({"writes": dict(write.ordered_writes()), "deletes": write.deletes, "retired": write.retired},
                      ensure_ascii=False)


def pending_journal(text: str) -> FolderWrite | None:
    """A journal's write; ``None`` when it does not read (a crash while writing it leaves no journal)."""
    try:
        data = json.loads(text)
        return FolderWrite(writes=dict(data["writes"]), deletes=list(data["deletes"]), retired=list(data["retired"]))
    except (ValueError, KeyError, TypeError):
        return None


def applied(folder: Folder, write: FolderWrite) -> Folder:
    """``folder`` as it reads once ``write`` completes."""
    texts = {**folder.texts, **dict(write.ordered_writes())}
    for file in write.deletes:
        texts.pop(file, None)
    problems = {f: p for f, p in folder.problems.items() if f not in texts and f not in write.deletes}
    return folder.model_copy(update={"texts": texts, "problems": problems,
                                     "retired": sorted({*folder.retired, *write.retired})})


class ModelEdit:
    """A model being edited: its resolved document (edit ``document`` in place) and where each dataset is stored."""

    def __init__(self, *, resolved: Resolved | None, name: str, version: str, split: bool) -> None:
        self.name = name
        self.resolved = resolved
        self.file = resolved.file if resolved is not None else None
        self.document: dict[str, Any] = copy.deepcopy(resolved.document) if resolved is not None else {
            "version": version, "name": name, "datasets": []}
        self.inline: set[str] = set(resolved.inline) if resolved is not None else set()
        self.refs: dict[str, Ref] = dict(resolved.refs) if resolved is not None else {}
        self.split = split

    @property
    def version(self) -> str:
        return str(self.document.get("version"))

    def datasets(self) -> list[dict[str, Any]]:
        return self.document.setdefault("datasets", [])

    def dataset(self, name: str) -> dict[str, Any] | None:
        """Dataset ``name``: the one spelled exactly so, else the one matching it ignoring case."""
        return next((d for d in self.datasets() if d.get("name") == name), None) or next(
            (d for d in self.datasets() if str(d.get("name")).casefold() == name.casefold()), None)

    def take(self, name: str) -> tuple[dict[str, Any], Ref | None, bool]:
        """Remove dataset ``name``: its entry, its reference and whether it was inline."""
        entry = self.dataset(name)
        assert entry is not None, name
        self.document["datasets"] = [d for d in self.datasets() if d is not entry]
        folded = name.casefold()
        inline = folded in self.inline
        self.inline.discard(folded)
        return entry, self.refs.pop(folded, None), inline

    def give(self, entry: dict[str, Any], *, ref: Ref | None, inline: bool) -> None:
        """Add a dataset moved from another model, stored as it was there."""
        folded = str(entry.get("name")).casefold()
        self.datasets().append(entry)
        if ref is not None:
            self.refs[folded] = ref
        elif inline and not self.split:
            self.inline.add(folded)


class Workspace:
    """A folder's models edited together, planned as one :class:`FolderWrite`."""

    def __init__(self, folder: Folder) -> None:
        self.folder = folder
        self.layout = classify(folder)
        self._models: dict[str, ModelEdit] = {}
        self._deleted: dict[str, Resolved] = {}

    def names(self) -> list[str]:
        return [n for n in self.layout.models if n not in self._deleted] + [
            n for n in self._models if n not in self.layout.models]

    def model(self, name: str) -> ModelEdit | None:
        """Model ``name`` for editing; ``ModelResolutionError`` when it does not resolve."""
        if name in self._models:
            return self._models[name]
        if name in self._deleted or name not in self.layout.models:
            return None
        resolved = resolve(self.folder, self.layout.models[name])
        found = ModelEdit(resolved=resolved, name=name, version="",
                          split=resolved.split or name == self.folder.data_source)
        self._models[name] = found
        return found

    def create(self, name: str, *, version: str, split: bool) -> ModelEdit:
        edit = ModelEdit(resolved=None, name=name, version=version, split=split)
        self._models[name] = edit
        return edit

    def delete(self, name: str) -> None:
        """Delete model ``name`` with its dataset documents (only its model document when it does not resolve)."""
        if name in self.layout.unreadable_files:
            file = self.layout.unreadable_files[name]
            self._deleted[name] = Resolved(name=name, file=file, stored=None, document={})
            return
        try:
            edit = self.model(name)
        except ModelResolutionError:
            file = self.layout.models[name]
            self._deleted[name] = Resolved(name=name, file=file, stored=None, document={})
            return
        if edit is not None and edit.resolved is not None:
            self._deleted[name] = edit.resolved
        self._models.pop(name, None)

    def owner(self, dataset: str) -> str | None:
        """The model holding ``dataset`` (case-insensitive), among the models that resolve."""
        for name in self.names():
            try:
                edit = self.model(name)
            except ModelResolutionError:
                continue
            if edit is not None and edit.dataset(dataset) is not None:
                return name
        return None

    def plan(self, *, retired: Iterable[str] = ()) -> FolderWrite:
        write = FolderWrite(retired=list(retired))
        taken = set(self.folder.entries())
        kept_files: set[str] = set()
        dropped_files: list[str] = []
        for name, resolved in self._deleted.items():
            write.deletes.append(resolved.file)
            dropped_files += [r.file for r in resolved.refs.values()]
        for edit in self._models.values():
            if not edit.datasets():
                if edit.resolved is not None:
                    write.deletes.append(edit.resolved.file)
                    dropped_files += [r.file for r in edit.resolved.refs.values()]
                continue
            self._plan_model(edit, write=write, taken=taken)
            kept_files |= {r.file for r in edit.refs.values()}
            if edit.resolved is not None:
                dropped_files += [r.file for r in edit.resolved.refs.values()]
        write.deletes += [f for f in dict.fromkeys(dropped_files) if f not in kept_files and f in self.folder.texts]
        write.deletes = list(dict.fromkeys(write.deletes))
        return write

    def _plan_model(self, edit: ModelEdit, *, write: FolderWrite, taken: set[str]) -> None:
        present = {str(d.get("name")).casefold() for d in edit.datasets()}
        edit.refs = {k: r for k, r in edit.refs.items() if k in present}
        edit.inline &= present
        inline: list[dict[str, Any]] = []
        refs: list[dict[str, str]] = []
        for entry in edit.datasets():
            name = str(entry.get("name"))
            folded = name.casefold()
            ref = edit.refs.get(folded)
            if ref is None and (folded in edit.inline or not edit.split):
                edit.inline.add(folded)
                inline.append(entry)
                continue
            if ref is None:
                file = file_name(name, suffix=DATASET_SUFFIX, taken=taken)
                taken.add(file)
                ref = edit.refs[folded] = Ref(name=name, iri=iri_of(file), file=file)
            refs.append({"name": name, "iri": ref.iri})
            self._plan_dataset(ref.file, entry, version=edit.version, write=write)
        stored = edit.resolved.stored if edit.resolved is not None else None
        model = _model_document(edit.document, stored=stored, inline=inline, refs=refs)
        if edit.resolved is None:
            file = file_name(edit.name, suffix=MODEL_SUFFIX, taken=taken)
            taken.add(file)
            edit.file = file
            write.model_writes[file] = dump(model)
        elif model != stored:
            write.model_writes[edit.resolved.file] = edit_text(self.folder.texts[edit.resolved.file],
                                                               lambda _doc: model)

    def _plan_dataset(self, file: str, entry: dict[str, Any], *, version: str, write: FolderWrite) -> None:
        text = self.folder.texts.get(file)
        held, _ = parsed_shared(text) if text is not None else (None, [])
        wanted = {"version": version, "dataset": entry}
        if text is None:
            write.writes[file] = dump(wanted)
        elif held != wanted:
            write.writes[file] = edit_text(text, lambda _doc: wanted)


def _model_document(document: dict[str, Any], *, stored: Any, inline: list[dict[str, Any]],
                    refs: list[dict[str, str]]) -> dict[str, Any]:
    """The model document holding ``document``'s model-level content, ``inline`` datasets and ``refs``; their keys
    keep the stored document's places."""
    order = list(stored) if isinstance(stored, dict) else list(document)
    if "datasets" not in order and "dataset_refs" not in order:
        order.append("datasets")
    if "dataset_refs" not in order:
        order.insert(order.index("datasets") + 1, "dataset_refs")
    elif "datasets" not in order:
        order.insert(order.index("dataset_refs"), "datasets")
    parts = {"datasets": inline, "dataset_refs": refs}
    out: dict[str, Any] = {}
    seen: set[str] = set()
    for key in [*order, *document]:
        if key in seen:
            continue
        seen.add(key)
        if key in parts:
            if parts[key]:
                out[key] = parts[key]
        elif key in document:
            out[key] = document[key]
    return out
