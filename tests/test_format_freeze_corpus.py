"""Spec: models/stored-format — the format-freeze corpus loads with no migration.

Corpus layout: ``tests/fixtures/format_freeze/yaml/`` is a YAML store root and ``tests/fixtures/format_freeze/store.db``
a SQLite store, each written in the stored format, with its datasources registered (no live database needed).
"""

from __future__ import annotations

import re
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._stored_format_fixtures import (
    STORAGES, dataset_documents, model_documents, recorded, refs, slayer_entry, snapshot, texts,
    upgrade_warning_names,
)

CORPUS = Path(__file__).parent / "fixtures" / "format_freeze"
FILE_SAFE = re.compile(r"^[A-Za-z0-9_-]+$")
PORTABLE_DIALECTS = {"ANSI_SQL", "OSSIE_SQL_2026"}

REQUIRED = {
    "table source", "query-backed source in SLAYER", "row filter in SLAYER",
    "composite primary_key", "multi-field unique_keys",
    *(f"foreign extension on {level}" for level in ("model", "dataset", "field", "metric", "relationship")),
    *(f"ai_context on {level}" for level in ("model", "dataset", "field", "metric", "relationship")),
    "is_time true", "is_time false", "is_time unset",
    "dialect entry SLayer cannot run",
    "datatype Decimal", "datatype Time", "datatype DateTimeTz", "list type as Opaque",
    "field label", "metric label in SLAYER",
    "datasource model", "split named model", "flat named model", "name that is not a file name",
}


def corpus_store(kind: str, tmp: Path) -> tuple[StorageBackend, Path]:
    """A copy of the committed ``kind`` store, opened; and the base ``texts`` reads it under."""
    assert CORPUS.is_dir(), f"the format-freeze corpus is missing: {CORPUS}"
    if kind == "yaml":
        shutil.copytree(CORPUS / "yaml", tmp / "store")
        return YAMLStorage(base_dir=str(tmp / "store")), tmp
    shutil.copyfile(CORPUS / "store.db", tmp / "store.db")
    return SQLiteStorage(db_path=str(tmp / "store.db")), tmp


async def all_texts(storage: StorageBackend, *, kind: str, base: Path) -> dict[str, dict[str, str]]:
    return {ds: texts(kind, base, data_source=ds) for ds in await storage.list_datasources()}


async def load_everything(storage: StorageBackend) -> list[Any]:
    """Load every datasource, model and dataset; the load failures."""
    failures: list[Any] = []
    for ds in await storage.list_datasources():
        assert await storage.get_datasource(ds) is not None
        for _, name in await storage.list_semantic_models(ds):
            assert await storage.get_semantic_model_text(name, data_source=ds) is not None
        for name in await storage.list_datasets(data_source=ds):
            assert await storage.get_dataset(name, data_source=ds) is not None
        _, errs = await storage.load_datasets(data_source=ds)
        failures += errs
    return failures


def _keys(node: Any) -> Iterator[tuple[str, Any]]:
    if isinstance(node, dict):
        for key, value in node.items():
            yield key, value
            yield from _keys(value)
    elif isinstance(node, list):
        for value in node:
            yield from _keys(value)


def _has(node: Any, key: str) -> bool:
    return any(k == key and v not in (None, [], {}, "") for k, v in _keys(node))


def _foreign(entity: dict[str, Any]) -> bool:
    return any(e.get("vendor_name") != "SLAYER" for e in entity.get("nonportable_extensions") or [])


def _entity_features(entity: dict[str, Any], *, level: str) -> set[str]:
    out = {f"ai_context on {level}"} if entity.get("ai_context") is not None else set()
    return out | ({f"foreign extension on {level}"} if _foreign(entity) else set())


def _field_features(entry: dict[str, Any], *, runnable: set[str]) -> set[str]:
    out = _entity_features(entry, level="field")
    is_time = (entry.get("dimension") or {}).get("is_time")
    out.add({True: "is_time true", False: "is_time false", None: "is_time unset"}[is_time])
    if any(d["dialect"] not in runnable for d in (entry.get("expression") or {}).get("dialects") or []):
        out.add("dialect entry SLayer cannot run")
    if entry.get("datatype") in ("Decimal", "Time", "DateTimeTz"):
        out.add(f"datatype {entry['datatype']}")
    if entry.get("datatype") == "Opaque" and _has(slayer_entry(entry), "element_type"):
        out.add("list type as Opaque")
    return out | ({"field label"} if entry.get("label") else set())


def _dataset_features(entry: dict[str, Any], *, runnable: set[str]) -> set[str]:
    out = _entity_features(entry, level="dataset")
    payload = slayer_entry(entry)
    if _has(payload, "source_queries"):
        out.add("query-backed source in SLAYER")
    elif isinstance(entry.get("source"), str):
        out.add("table source")
    if _has(payload, "filters"):
        out.add("row filter in SLAYER")
    if len(entry.get("primary_key") or []) > 1:
        out.add("composite primary_key")
    if any(len(k) > 1 for k in entry.get("unique_keys") or []):
        out.add("multi-field unique_keys")
    for item in entry.get("fields") or []:
        out |= _field_features(item, runnable=runnable)
    for item in entry.get("metrics") or []:
        out |= _metric_features(item)
    return out


def _metric_features(entry: dict[str, Any]) -> set[str]:
    return _entity_features(entry, level="metric") | (
        {"metric label in SLAYER"} if _has(slayer_entry(entry), "label") else set())


async def covered(storage: StorageBackend, *, kind: str, base: Path) -> set[str]:
    """Every construct of the stored-format construct list the stored documents hold."""
    out: set[str] = set()
    for ds in await storage.list_datasources():
        config = await storage.get_datasource(ds)
        runnable = PORTABLE_DIALECTS | {(config.type or "").upper() if config else ""}
        datasets = dataset_documents(kind, base, data_source=ds)
        for file_name, doc in datasets.items():
            out |= _dataset_features(doc["dataset"], runnable=runnable)
            if not FILE_SAFE.match(doc["dataset"]["name"]) and "%" in file_name:
                out.add("name that is not a file name")
        for name, doc in model_documents(kind, base, data_source=ds).items():
            out |= _entity_features(doc, level="model")
            for entry in doc.get("datasets") or []:
                out |= _dataset_features(entry, runnable=runnable)
            for entry in doc.get("relationships") or []:
                out |= _entity_features(entry, level="relationship")
            for entry in doc.get("metrics") or []:
                out |= _metric_features(entry)
            if name == ds and refs(doc):
                out.add("datasource model")
            elif name != ds:
                out.add("split named model" if refs(doc) else "flat named model")
            if not FILE_SAFE.match(name):
                out.add("name that is not a file name")
    return out


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_corpus_covers_every_construct(tmp_path: Path, kind: str) -> None:
    storage, base = corpus_store(kind, tmp_path)
    assert REQUIRED - await covered(storage, kind=kind, base=base) == set()


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_corpus_is_current(tmp_path: Path, kind: str) -> None:
    """Spec scenario: The corpus is current."""
    storage, base = corpus_store(kind, tmp_path)
    before = await all_texts(storage, kind=kind, base=base)
    files = snapshot(tmp_path)
    assert before and all(before.values())
    with recorded() as record:
        assert await load_everything(storage) == []
    assert upgrade_warning_names(record) == []
    for ds in await storage.list_datasources():
        for name in await storage.list_datasets(data_source=ds):
            dataset = await storage.get_dataset(name, data_source=ds)
            assert dataset is not None
            await storage.save_dataset(dataset, _validate=False)
        for _, name in await storage.list_semantic_models(ds):
            text = await storage.get_semantic_model_text(name, data_source=ds)
            assert text is not None
            await storage.save_semantic_model_text(data_source=ds, name=name, text=text)
    assert await all_texts(storage, kind=kind, base=base) == before
    if kind == "yaml":
        after = snapshot(tmp_path)
        assert {k: v for k, v in after.items() if "semantic_models" in k} == {
            k: v for k, v in files.items() if "semantic_models" in k}

