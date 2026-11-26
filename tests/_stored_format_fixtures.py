"""Shared helpers for the stored-format tests: store layout readers, warning capture and write interruption.

Documents are found by content, never by file name, except where a test pins a file name.
"""

from __future__ import annotations

import contextlib
import json
import os
import warnings
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import jsonschema
import yaml

from slayer.core.models import DatasetField, DatasetRelationship, DatasourceConfig, SlayerDataset
from slayer.core.warnings import SlayerWarning
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import open_connection
from tests._dev2243_fixtures import DS, STORAGES, make_storage
from tests._osi_helpers import SCHEMA_PATH

__all__ = ["DS", "STORAGES", "make_storage"]


# --------------------------------------------------------------------------- #
# Stores
# --------------------------------------------------------------------------- #
def folder(base: Path, *, data_source: str = DS) -> Path:
    """The datasource folder of the YAML store ``make_storage("yaml", base)``."""
    return base / "store" / "semantic_models" / data_source


def legacy_folder(base: Path, *, data_source: str = DS) -> Path:
    """The 2.0 standalone-dataset folder of the YAML store ``make_storage("yaml", base)``."""
    return base / "store" / "models" / data_source


def sqlite_path(base: Path) -> Path:
    return base / "store.db"


async def store_with_datasource(kind: str, base: Path, *, data_source: str = DS, ds_type: str = "duckdb",
                                database: str | None = None) -> StorageBackend:
    """A store with ``data_source`` registered (its database not created unless ``database`` is given)."""
    storage = make_storage(kind, base)
    await storage.save_datasource(DatasourceConfig(name=data_source, type=ds_type,
                                                   database=database or str(base / "absent.duckdb")))
    return storage


def reopen(kind: str, base: Path) -> StorageBackend:
    """A fresh storage instance over the same files (no cached state)."""
    return make_storage(kind, base)


def texts(kind: str, base: Path, *, data_source: str = DS) -> dict[str, str]:
    """``{file name: text}`` of every stored document of ``data_source``; dot-files (temporaries, journals) and lock
    files excluded."""
    if kind == "yaml":
        root = folder(base, data_source=data_source)
        if not root.is_dir():
            return {}
        return {p.name: p.read_text(encoding="utf-8") for p in sorted(root.iterdir())
                if p.is_file() and not p.name.startswith(".") and p.suffix != ".lock"}
    with open_connection(sqlite_path(base)) as con:
        rows = con.execute("SELECT file_name, text FROM documents WHERE data_source = ? ORDER BY file_name",
                           (data_source,)).fetchall()
    return {name: text for name, text in rows}


def parse(text: str) -> dict[str, Any]:
    return json.loads(text) if text.lstrip().startswith("{") else yaml.safe_load(text)


def model_documents(kind: str, base: Path, *, data_source: str = DS) -> dict[str, dict[str, Any]]:
    """``{model name: parsed model document}`` of every model document of ``data_source``."""
    out: dict[str, dict[str, Any]] = {}
    for text in texts(kind, base, data_source=data_source).values():
        doc = parse(text)
        if isinstance(doc, dict) and "dataset" not in doc and ("datasets" in doc or "dataset_refs" in doc):
            out[doc["name"]] = doc
    return out


def model_file(kind: str, base: Path, name: str, *, data_source: str = DS) -> str:
    """The file name of model document ``name``."""
    hits = [f for f, t in texts(kind, base, data_source=data_source).items()
            if (d := parse(t)) and "dataset" not in d and d.get("name") == name]
    assert len(hits) == 1, f"model {name!r}: {hits}"
    return hits[0]


def dataset_documents(kind: str, base: Path, *, data_source: str = DS) -> dict[str, dict[str, Any]]:
    """``{file name: parsed dataset document}`` of every dataset document of ``data_source``."""
    return {f: d for f, t in texts(kind, base, data_source=data_source).items()
            if isinstance(d := parse(t), dict) and "dataset" in d}


def dataset_file(kind: str, base: Path, name: str, *, data_source: str = DS) -> str:
    """The file name of the dataset document declaring dataset ``name`` (exact spelling)."""
    hits = [f for f, d in dataset_documents(kind, base, data_source=data_source).items() if d["dataset"]["name"] == name]
    assert len(hits) == 1, f"dataset {name!r}: {hits}"
    return hits[0]


def stored_dataset(kind: str, base: Path, name: str, *, data_source: str = DS) -> dict[str, Any]:
    """The stored Ossie dataset ``name``, from its dataset document or inline in a model document."""
    for doc in dataset_documents(kind, base, data_source=data_source).values():
        if doc["dataset"]["name"] == name:
            return doc["dataset"]
    for doc in model_documents(kind, base, data_source=data_source).values():
        for entry in doc.get("datasets") or []:
            if entry["name"] == name:
                return entry
    raise AssertionError(f"dataset {name!r} is not stored in {data_source!r}")


def refs(doc: dict[str, Any]) -> list[str]:
    """The ``iri`` of every ``dataset_refs`` entry of a model document."""
    return [r["iri"] for r in doc.get("dataset_refs") or []]


def slayer_entry(entity: dict[str, Any]) -> dict[str, Any] | None:
    """The ``data`` of ``entity``'s ``SLAYER`` extension entry, parsed."""
    for entry in entity.get("nonportable_extensions") or []:
        if entry.get("vendor_name") == "SLAYER":
            data = entry["data"]
            return json.loads(data) if isinstance(data, str) else data
    return None


def snapshot(root: Path) -> dict[str, bytes]:
    """Every file under ``root`` with its bytes."""
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def legacy_rows(base: Path, *, table: str = "models") -> list[tuple[str, str]]:
    """``(data_source, name)`` of every row of a 2.0 SQLite table; ``[]`` when the table is gone."""
    with open_connection(sqlite_path(base)) as con:
        exists = con.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
        if not exists:
            return []
        return [tuple(r) for r in con.execute(f"SELECT data_source, name FROM {table}")]  # NOSONAR(S2077) — test


# --------------------------------------------------------------------------- #
# Datasets
# --------------------------------------------------------------------------- #
def table_dataset(name: str, *, data_source: str = DS, sql_table: str | None = None, fields: list[str] | None = None,
                  primary_key: list[str] | None = None, **extra: Any) -> SlayerDataset:
    """A table-backed dataset whose fields read same-named columns."""
    names = fields or ["id"]
    pk = set(primary_key or [])
    return SlayerDataset(name=name, data_source=data_source, sql_table=sql_table or name,
                         fields=[DatasetField(name=n, primary_key=n in pk) for n in names], **extra)


def link(to: str, *, from_columns: list[str], to_columns: list[str], name: str | None = None) -> DatasetRelationship:
    return DatasetRelationship(to=to, from_columns=from_columns, to_columns=to_columns, name=name)


# --------------------------------------------------------------------------- #
# Warnings
# --------------------------------------------------------------------------- #
@contextlib.contextmanager
def recorded() -> Generator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        yield record


def payloads(record: list[warnings.WarningMessage]) -> list[SlayerWarning]:
    """Every structured warning payload in ``record``."""
    return [p for w in record if isinstance(p := getattr(w.message, "payload", None), SlayerWarning)]


def messages(record: list[warnings.WarningMessage]) -> list[str]:
    """Every warning's text: structured payloads by their human message, others by their string."""
    out = [p.human_message() for p in payloads(record)]
    out += [str(w.message) for w in record if getattr(w.message, "payload", None) is None]
    return out


def upgrade_warning_names(record: list[warnings.WarningMessage]) -> list[str]:
    return [p.name for p in payloads(record) if getattr(p, "kind", None) == "stored_upgrade_needed"]  # type: ignore[attr-defined]


# --------------------------------------------------------------------------- #
# Ossie validity
# --------------------------------------------------------------------------- #
def schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def validate_document(doc: dict[str, Any]) -> None:
    """Raise unless ``doc`` passes the vendored core schema."""
    jsonschema.Draft202012Validator(schema()).validate(doc)


def validate_dataset(entry: dict[str, Any]) -> None:
    """Raise unless ``entry`` passes the vendored schema's ``Dataset`` definition."""
    full = schema()
    jsonschema.Draft202012Validator({"$schema": full["$schema"], "$defs": full["$defs"],
                                     "$ref": "#/$defs/Dataset"}).validate(entry)


# --------------------------------------------------------------------------- #
# Write interruption
# --------------------------------------------------------------------------- #
class Interrupted(BaseException):
    """Raised in place of a file operation, as a crash would stop the process."""


_FILE_OPS = ("replace", "rename", "unlink", "remove")


@contextlib.contextmanager
def file_ops(root: Path, *, stop_after: int | None = None) -> Generator[list[str]]:
    """Record every ``os`` replace/rename/unlink/remove whose target lies under ``root``; with ``stop_after``, raise
    :class:`Interrupted` instead of the operation following the first ``stop_after`` ones. Stored files are written by
    atomic replace, so these are the writes a crash can split."""
    seen: list[str] = []
    originals: dict[str, Callable[..., Any]] = {op: getattr(os, op) for op in _FILE_OPS}
    resolved = str(root.resolve())

    def wrap(op: str) -> Callable[..., Any]:
        original = originals[op]

        def run(*args: Any, **kwargs: Any) -> Any:
            target = str(Path(args[-1] if op in ("replace", "rename") else args[0]).resolve())
            if target.startswith(resolved):
                if stop_after is not None and len(seen) >= stop_after:
                    raise Interrupted(f"{op} {target}")
                seen.append(f"{op} {target}")
            return original(*args, **kwargs)

        return run

    try:
        for op in _FILE_OPS:
            setattr(os, op, wrap(op))
        yield seen
    finally:
        for op, original in originals.items():
            setattr(os, op, original)
