"""Ontology and mapping hooks default to unsupported: a backend without them keeps working and fails typed.

Spec: storage/custom-backends › Ontology and mapping hooks default to unsupported.
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

import pytest

from slayer.core.models import DatasourceConfig
from slayer.storage.base import _STORAGE_REGISTRY, register_storage, resolve_storage
from tests._dev2243_fixtures import SALES, store_document
from tests._dev2474_fixtures import FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, example, raises_refusal, refusal
from tests.test_dev2243_custom_backend import DocumentBackend, _mcp

UNSUPPORTED = "does not support ontology documents"
ERROR = "OntologiesUnsupportedError"


@pytest.fixture
def registered() -> Generator[str]:
    """``ontodict://`` resolves to a fresh backend implementing every hook but the ontology ones."""
    register_storage("ontodict", lambda _rest: DocumentBackend())
    try:
        yield "ontodict://store"
    finally:
        _STORAGE_REGISTRY.pop("ontodict", None)


async def test_a_backend_without_the_ontology_hooks(registered: str, tmp_path: Path) -> None:
    """Spec scenario: A backend without the ontology hooks."""
    storage = resolve_storage(registered)
    await storage.save_datasource(DatasourceConfig(name="shop", type="duckdb", database=str(tmp_path / "x.duckdb")))
    await store_document(storage, SALES)
    assert await storage.get_semantic_model_text("sales", "shop") == SALES
    loaded, errors = await storage.load_datasets(data_source="shop")
    assert sorted(d.name for d in loaded) == ["customers", "orders"]
    assert errors == []

    assert await storage.list_ontologies() == []
    assert await storage.list_ontology_mappings() == []
    assert await storage.list_ontology_mappings("shop") == []

    reply = await _mcp(storage, "create_ontology", content=example("flights.ontology.yaml"))
    assert ERROR in reply
    assert "DocumentBackend" in reply
    assert UNSUPPORTED in reply


@pytest.mark.parametrize("operation", ["save", "get", "delete", "save_mapping", "get_mapping", "delete_mapping"])
async def test_every_ontology_read_and_write_fails_typed(registered: str, operation: str) -> None:
    storage = resolve_storage(registered)
    calls = {
        "save": lambda: storage.save_ontology_text(name=FLIGHTS, text=example("flights.ontology.yaml")),
        "get": lambda: storage.get_ontology_text(FLIGHTS),
        "delete": lambda: storage.delete_ontology(FLIGHTS),
        "save_mapping": lambda: storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                                                   text=example("flights.mapping.yaml")),
        "get_mapping": lambda: storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS),
        "delete_mapping": lambda: storage.delete_ontology_mapping(FLIGHTS_MAPPING, FLIGHTS_DS),
    }
    with raises_refusal(ERROR) as info:
        await calls[operation]()
    assert "DocumentBackend" in refusal(info.value)
    assert UNSUPPORTED in refusal(info.value)


async def test_a_datasource_delete_still_works(registered: str, tmp_path: Path) -> None:
    storage = resolve_storage(registered)
    await storage.save_datasource(DatasourceConfig(name="shop", type="duckdb", database=str(tmp_path / "x.duckdb")))
    assert await storage.delete_datasource("shop") is True
