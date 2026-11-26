"""Spec: models/stored-format — every stored model is Ossie-valid."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import jsonschema
import pytest
import yaml

from slayer.storage.base import StorageBackend
from tests._dev2243_fixtures import CORPUS, SALES, store_document, upstream_validate
from tests._stored_format_fixtures import (
    DS, STORAGES, dataset_documents, link, model_documents, refs, store_with_datasource, table_dataset, texts,
    validate_dataset, validate_document,
)

SPLIT = "support"
FLAT = "sales"


async def mixed_store(kind: str, base: Path) -> StorageBackend:
    """A datasource model with two dataset documents, the split model ``support`` and the flat model ``sales``."""
    base.mkdir(parents=True, exist_ok=True)
    storage = await store_with_datasource(kind, base)
    managers = table_dataset("managers", fields=["manager_id", "name"], primary_key=["manager_id"])
    stores = table_dataset("stores", fields=["store_id", "customer_id", "manager_id"], primary_key=["store_id"],
                           relationships=[link("managers", from_columns=["manager_id"], to_columns=["manager_id"])])
    returns = table_dataset("returns", fields=["return_id", "customer_id", "amount"], primary_key=["return_id"])
    for dataset in (managers, stores, returns):
        await storage.save_dataset(dataset, _validate=False)
    await storage.create_semantic_model(data_source=DS, name=SPLIT, datasets=["returns"])
    await store_document(storage, SALES)
    return storage


def resolved(kind: str, base: Path) -> dict[str, dict[str, Any]]:
    """Every stored model with each ``dataset_refs`` entry's dataset inlined into ``datasets``."""
    files = dataset_documents(kind, base)
    out: dict[str, dict[str, Any]] = {}
    for name, stored in model_documents(kind, base).items():
        doc = copy.deepcopy(stored)
        inlined = [files[unquote(iri)]["dataset"] for iri in refs(doc)]
        doc.pop("dataset_refs", None)
        doc["datasets"] = [*(doc.get("datasets") or []), *inlined]
        out[name] = doc
    return out


@pytest.mark.parametrize("kind", STORAGES)
async def test_every_stored_model_resolves_and_passes_the_core_schema(tmp_path: Path, kind: str) -> None:
    await mixed_store(kind, tmp_path)
    models = model_documents(kind, tmp_path)
    assert set(models) == {DS, SPLIT, FLAT}
    assert refs(models[DS]) and refs(models[SPLIT])
    assert "dataset_refs" not in models[FLAT]
    documents = resolved(kind, tmp_path)
    assert sorted(d["name"] for d in documents[DS]["datasets"]) == ["managers", "stores"]
    assert [d["name"] for d in documents[SPLIT]["datasets"]] == ["returns"]
    for doc in documents.values():
        validate_document(doc)
    datasets = dataset_documents(kind, tmp_path)
    assert sorted(d["dataset"]["name"] for d in datasets.values()) == ["managers", "returns", "stores"]
    for doc in datasets.values():
        assert set(doc) == {"version", "dataset"}
        validate_dataset(doc["dataset"])


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_store_passes_upstreams_validator(tmp_path: Path, kind: str) -> None:
    """Spec scenario: A store passes upstream's validator."""
    await mixed_store(kind, tmp_path / "s")
    documents = resolved(kind, tmp_path / "s")
    assert set(documents) == {DS, SPLIT, FLAT}
    paths = []
    for name, doc in sorted(documents.items()):
        path = tmp_path / "resolved" / f"{len(paths)}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8")
        paths.append(path)
        assert doc["name"] == name
    done = upstream_validate(paths, tmp=tmp_path)
    assert done.returncode == 0, done.stdout + done.stderr


def _corpus_schemas() -> list[dict[str, Any]]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(CORPUS.rglob("*.json"))
            if "schema" in p.name]


def split_schemas() -> tuple[dict[str, Any], dict[str, Any]]:
    """The vendored multi-file schemas: one accepting ``dataset_refs`` in a model, one for a dataset document."""
    schemas = _corpus_schemas()
    model = next((s for s in schemas if "dataset_refs" in (s.get("properties") or {})), None)
    dataset = next((s for s in schemas if "dataset" in (s.get("required") or [])), None)
    if model is None or dataset is None:
        pytest.fail("Ossie multi-file model schemas are not in the vendored corpus")
    return model, dataset


@pytest.mark.xfail(strict=True, reason="Ossie multi-file models are not vendored yet; DEV-2484 re-vendors them")
@pytest.mark.parametrize("kind", STORAGES)
async def test_stored_folders_pass_upstreams_own_multi_file_resolution(tmp_path: Path, kind: str) -> None:
    model_schema, dataset_schema = split_schemas()
    await mixed_store(kind, tmp_path / "s")
    folder = tmp_path / "folder"
    folder.mkdir()
    for name, text in texts(kind, tmp_path / "s").items():
        (folder / name).write_text(text, encoding="utf-8")
    for doc in model_documents(kind, tmp_path / "s").values():
        jsonschema.Draft202012Validator(model_schema).validate(doc)
    for doc in dataset_documents(kind, tmp_path / "s").values():
        jsonschema.Draft202012Validator(dataset_schema).validate(doc)
    models = [folder / f for f, t in texts(kind, tmp_path / "s").items() if "dataset" not in (yaml.safe_load(t) or {})]
    done = upstream_validate(models, tmp=tmp_path)
    assert done.returncode == 0, done.stdout + done.stderr
