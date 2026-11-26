"""Documents SLayer does not own upgrade without a store, as ``slayer storage upgrade`` would store them.

Spec: models/stored-upgrade › Documents SLayer does not own upgrade without a store; compatibility/integrator-1x-calls
› Model objects read through their 1.x names (Upgraded files pass a version and key check).
"""

from __future__ import annotations

import asyncio
import copy
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.upgrade import upgrade_datasets
from slayer.storage.yaml_storage import YAMLStorage
from tests._integrator_1x import DATA_SOURCE, raw_documents
from tests._storage_upgrade_cli import store_path, upgrade, warehouse_store
from tests._stored_upgrade_fixtures import assert_current, ossie_document


def _unknown_keys(raw_doc: dict[str, Any], owner: type[BaseModel]) -> list[str]:
    """Keys a model declares no field (or alias) for, at any depth (the integrator's unknown-key walk)."""
    accepted: dict[str, Any] = {}
    for name, info in owner.model_fields.items():
        for spelling in (name, info.alias):
            if isinstance(spelling, str):
                accepted[spelling] = info.annotation
    found: list[str] = []
    for key, value in raw_doc.items():
        if key not in accepted:
            found.append(key)
            continue
        nested = _model_in(accepted[key])
        if nested is None:
            continue
        items = value if isinstance(value, list) else [value]
        found.extend(k for item in items if isinstance(item, dict) for k in _unknown_keys(item, nested))
    return found


def _model_in(annotation: Any) -> type[BaseModel] | None:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    for arg in getattr(annotation, "__args__", ()):
        found = _model_in(arg)
        if found is not None:
            return found
    return None


def test_a_v15_model_set_upgrades_in_memory(tmp_path: Path) -> None:
    documents = list(raw_documents().values())
    pristine = copy.deepcopy(documents)
    result = upgrade_datasets(documents=documents, datasource_type="duckdb")
    assert documents == pristine
    assert result.refused == []
    assert [d["name"] for d in result.documents] == ["orders", "customers"]
    for document in result.documents:
        assert document["version"] == CURRENT_VERSIONS["SlayerModel"]
        assert _unknown_keys(document, SlayerDataset) == []
        yaml.safe_dump(document)
    store = warehouse_store(backend="yaml", base=str(tmp_path / "store"))
    for document in result.documents:
        loaded = asyncio.run(store.get_dataset(document["name"], data_source=DATA_SOURCE))
        assert loaded is not None
        assert SlayerDataset.model_validate(document).model_dump(mode="json") == loaded.model_dump(mode="json")


def test_the_result_matches_what_the_store_upgrade_stores(tmp_path: Path) -> None:
    base = str(tmp_path / "store")
    storage = warehouse_store(backend="yaml", base=base)
    assert upgrade(store_path(storage)).code == 0
    reopened = YAMLStorage(base_dir=base)
    result = upgrade_datasets(documents=list(raw_documents().values()), datasource_type="duckdb")
    for document in result.documents:
        assert_current(ossie_document(backend="yaml", base=base, name=document["name"], data_source=DATA_SOURCE))
        loaded = asyncio.run(reopened.get_dataset(document["name"], data_source=DATA_SOURCE))
        assert loaded is not None
        assert SlayerDataset.model_validate(document).model_dump(mode="json") == loaded.model_dump(mode="json")


def test_a_datasource_config_supplies_the_dialect() -> None:
    config = DatasourceConfig(name=DATA_SOURCE, type="duckdb")
    result = upgrade_datasets(documents=list(raw_documents().values()), datasource=config)
    assert result.refused == []
    orders = next(d for d in result.documents if d["name"] == "orders")
    assert "DUCKDB" in yaml.safe_dump(orders)


def test_a_document_waiting_for_its_dialect_is_refused() -> None:
    result = upgrade_datasets(documents=list(raw_documents().values()))
    assert [d["name"] for d in result.documents] == ["customers"]
    [refusal] = result.refused
    assert refusal.name == "orders"
    assert "dialect" in refusal.reason


def test_a_pre_v8_document_is_refused() -> None:
    v5 = {"version": 5, "name": "items", "sql_table": "items", "data_source": DATA_SOURCE,
          "columns": [{"name": "qty", "sql": "qty", "type": "number"}]}
    result = upgrade_datasets(documents=[v5], datasource_type="duckdb")
    assert result.documents == []
    [refusal] = result.refused
    assert refusal.name == "items"
    assert "refinement" in refusal.reason
    assert "slayer storage upgrade" in refusal.reason

