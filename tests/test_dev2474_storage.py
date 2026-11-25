"""Ontologies and mappings are stored verbatim, and explicit changes never break a stored mapping.

Spec: ontologies/ontology-documents › Ontologies and mappings are stored verbatim; Explicit changes never break a
stored mapping.
"""

from __future__ import annotations

import contextlib
import warnings
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import duckdb
import pytest

from slayer.core.models import DatasourceConfig
from slayer.storage.base import StorageBackend
from tests._dev2243_fixtures import STORAGES, make_storage
from tests._dev2474_fixtures import (
    DEP_DELAY_PATH, FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, SHOP_ONTOLOGY, component, edited,
    empty_store, example, flights_store, load, order_mapping, raises_refusal, refusal, shop_store,
)
from tests._engine_helpers import owned_query_engine

BREAK = "OntologyMappingBreakError"


def without_dep_delay(document: dict[str, Any]) -> None:
    [flight] = [d for d in document["datasets"] if d["name"] == "FLIGHT"]
    flight["fields"] = [f for f in flight["fields"] if f["name"] != "dep_delay"]


async def kept(storage: StorageBackend) -> None:
    """Ontology, model and mapping are all still stored, and the mapping resolves."""
    assert await storage.get_ontology_text(FLIGHTS) == example("flights.ontology.yaml")
    assert await storage.get_semantic_model_text(FLIGHTS_MODEL, FLIGHTS_DS) is not None
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == example("flights.mapping.yaml")
    assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []


# --------------------------------------------------------------------------- #
# Stored verbatim
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_documents_are_stored_and_read_as_their_text(tmp_path: Path, kind: str) -> None:
    storage = await flights_store(kind, tmp_path)
    assert await storage.list_ontologies() == [FLIGHTS]
    assert await storage.get_ontology_text(FLIGHTS) == example("flights.ontology.yaml")
    assert await storage.list_ontology_mappings() == [(FLIGHTS_DS, FLIGHTS_MAPPING)]
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == example("flights.mapping.yaml")


@pytest.mark.parametrize("kind", STORAGES)
async def test_saving_under_a_stored_name_replaces(tmp_path: Path, kind: str) -> None:
    storage = await flights_store(kind, tmp_path)
    ontology = example("flights.ontology.yaml").replace(
        "description: Ontology of flights into and out of airports.", "description: Replaced.", 1)
    mapping = example("flights.mapping.yaml").replace(
        "description: Maps the Flights ontology", "description: Replaced. Maps the Flights ontology", 1)
    await storage.save_ontology_text(name=FLIGHTS, text=ontology)
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING, text=mapping)
    assert await storage.get_ontology_text(FLIGHTS) == ontology
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == mapping
    assert await storage.list_ontologies() == [FLIGHTS]


@pytest.mark.parametrize("kind", STORAGES)
async def test_ontologies_are_global_and_mappings_per_datasource(tmp_path: Path, kind: str) -> None:
    storage = await flights_store(kind, tmp_path)
    await storage.save_datasource(DatasourceConfig(name="other", type="duckdb", database=str(tmp_path / "x.duckdb")))
    await storage.save_semantic_model_text(data_source="other", name=FLIGHTS_MODEL,
                                           text=example("flights.semantic_model.yaml"))
    await storage.save_ontology_mapping_text(data_source="other", name=FLIGHTS_MAPPING,
                                             text=example("flights.mapping.yaml"))
    assert await storage.list_ontologies() == [FLIGHTS]
    assert await storage.list_ontology_mappings() == [(FLIGHTS_DS, FLIGHTS_MAPPING), ("other", FLIGHTS_MAPPING)]
    assert await storage.list_ontology_mappings("other") == [("other", FLIGHTS_MAPPING)]


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_an_unmapped_ontology_and_a_mapping(tmp_path: Path, kind: str) -> None:
    storage = await flights_store(kind, tmp_path)
    assert await storage.delete_ontology_mapping(FLIGHTS_MAPPING, FLIGHTS_DS) is True
    assert await storage.delete_ontology_mapping(FLIGHTS_MAPPING, FLIGHTS_DS) is False
    assert await storage.delete_ontology(FLIGHTS) is True
    assert await storage.delete_ontology(FLIGHTS) is False
    assert await storage.list_ontologies() == []
    assert await storage.list_ontology_mappings() == []


async def test_yaml_storage_keeps_one_file_per_document(tmp_path: Path) -> None:
    await flights_store("yaml", tmp_path)
    store = tmp_path / "store"
    assert (store / "ontologies" / f"{FLIGHTS}.yaml").read_text(encoding="utf-8") == example("flights.ontology.yaml")
    assert (store / "ontology_mappings" / FLIGHTS_DS / f"{FLIGHTS_MAPPING}.yaml").read_text(
        encoding="utf-8") == example("flights.mapping.yaml")


@pytest.mark.parametrize("kind", STORAGES)
async def test_datasource_delete(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Datasource delete."""
    storage = await flights_store(kind, tmp_path)
    assert await storage.delete_datasource(FLIGHTS_DS) is True
    assert await storage.list_ontology_mappings() == []
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) is None
    assert await storage.list_ontologies() == [FLIGHTS]


@pytest.mark.parametrize("kind", STORAGES)
async def test_the_graph_fingerprint_follows_every_change(tmp_path: Path, kind: str) -> None:
    storage = await flights_store(kind, tmp_path, mapping=False)
    seen = [await storage.graph_fingerprint()]

    async def changed(step: Awaitable[Any]) -> None:
        await step
        seen.append(await storage.graph_fingerprint())
        assert seen[-1] is not None
        assert seen[-1] != seen[-2]

    await changed(storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                                     text=example("flights.mapping.yaml")))
    await changed(storage.save_ontology_text(name=FLIGHTS, text=example("flights.ontology.yaml").replace(
        "Ontology of flights", "An ontology of flights", 1)))
    await changed(storage.delete_ontology_mapping(FLIGHTS_MAPPING, FLIGHTS_DS))
    await changed(storage.delete_ontology(FLIGHTS))


# --------------------------------------------------------------------------- #
# Explicit changes never break a stored mapping
# --------------------------------------------------------------------------- #
async def _delete_ontology(storage: StorageBackend) -> None:
    await storage.delete_ontology(FLIGHTS)


async def _replace_ontology(storage: StorageBackend) -> None:
    await storage.save_ontology_text(name=FLIGHTS, text=edited(
        example("flights.ontology.yaml"), lambda d: d["ontology"].remove(component(d, "Delay"))))


async def _replace_model(storage: StorageBackend) -> None:
    await storage.save_semantic_model_text(data_source=FLIGHTS_DS, name=FLIGHTS_MODEL,
                                           text=edited(example("flights.semantic_model.yaml"), without_dep_delay))


async def _delete_model(storage: StorageBackend) -> None:
    await storage.delete_semantic_model(FLIGHTS_MODEL, data_source=FLIGHTS_DS)


async def _edit_dataset(storage: StorageBackend) -> None:
    flight = await storage.get_dataset("FLIGHT", FLIGHTS_DS)
    assert flight is not None
    await storage.save_dataset(flight.model_copy(update={"fields": [f for f in flight.fields
                                                                    if f.name != "dep_delay"]}))


async def _delete_dataset(storage: StorageBackend) -> None:
    await storage.delete_dataset("FLIGHT", FLIGHTS_DS)


@pytest.mark.parametrize("change", [_delete_ontology, _replace_ontology, _replace_model, _delete_model, _edit_dataset,
                                    _delete_dataset])
@pytest.mark.parametrize("kind", STORAGES)
async def test_an_explicit_change_breaking_a_mapping_is_refused(
        tmp_path: Path, kind: str, change: Callable[[StorageBackend], Awaitable[None]]) -> None:
    storage = await flights_store(kind, tmp_path)
    with raises_refusal(BREAK) as info:
        await change(storage)
    assert FLIGHTS_MAPPING in refusal(info.value)
    await kept(storage)


async def test_ontology_delete_is_refused(tmp_path: Path) -> None:
    """Spec scenario: Ontology delete is refused."""
    storage = await flights_store("yaml", tmp_path)
    with raises_refusal(BREAK) as info:
        await storage.delete_ontology(FLIGHTS)
    assert FLIGHTS_MAPPING in refusal(info.value)
    await kept(storage)


async def test_dataset_edit_is_refused(tmp_path: Path) -> None:
    """Spec scenario: Dataset edit is refused."""
    storage = await flights_store("yaml", tmp_path)
    with raises_refusal(BREAK) as info:
        await _edit_dataset(storage)
    found = refusal(info.value)
    assert FLIGHTS_MAPPING in found
    assert DEP_DELAY_PATH in found
    assert "dep_delay" in found
    await kept(storage)


async def test_a_refusal_lists_every_broken_mapping(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name="second_mapping",
                                             text=example("flights.mapping.yaml"))
    with raises_refusal(BREAK) as info:
        await storage.delete_ontology(FLIGHTS)
    found = refusal(info.value)
    assert FLIGHTS_MAPPING in found
    assert "second_mapping" in found


async def test_a_change_that_keeps_every_mapping_resolving_applies(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    flight = await storage.get_dataset("FLIGHT", FLIGHTS_DS)
    assert flight is not None
    await storage.save_dataset(flight.model_copy(update={"description": "A flight."}))
    assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []


async def test_a_change_elsewhere_is_not_checked_against_the_mapping(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    await storage.save_ontology_text(name="shop", text=SHOP_ONTOLOGY)
    assert await storage.delete_ontology("shop") is True


@pytest.mark.parametrize("kind", STORAGES)
async def test_deleting_a_mapping_is_always_allowed(tmp_path: Path, kind: str) -> None:
    storage = await shop_store(kind, tmp_path)
    await storage.save_ontology_mapping_text(data_source="shop", name="shop_mapping",
                                             text=order_mapping(semantic_model="shop_model"))
    assert await storage.delete_ontology_mapping("shop_mapping", "shop") is True
    assert await storage.delete_ontology("shop") is True


# --------------------------------------------------------------------------- #
# Database-driven changes warn and leave the mapping unresolved
# --------------------------------------------------------------------------- #
def _live_flights(base: Path) -> tuple[str, Path]:
    """The flights model over plain table names, and a DuckDB holding each of its tables except FLIGHTS.dep_delay."""
    model = example("flights.semantic_model.yaml").replace("DATABASE.SCHEMA.", "")
    database = base / "flights.duckdb"
    with contextlib.closing(duckdb.connect(str(database))) as con:
        for dataset in load(model)["datasets"]:
            columns = [f["expression"]["sql"] for f in dataset["fields"]
                       if not (dataset["name"] == "FLIGHT" and f["name"] == "dep_delay")]
            quoted = ", ".join(f'"{c}" VARCHAR' for c in columns)
            con.execute(f"CREATE TABLE {dataset['source']} ({quoted})")
    return model, database


@pytest.mark.parametrize("kind", STORAGES)
async def test_live_schema_drops_a_field(tmp_path: Path, kind: str) -> None:
    """Spec scenario: Live schema drops a field."""
    model, database = _live_flights(tmp_path)
    storage = await flights_store(kind, tmp_path, model=model)
    await storage.save_datasource(DatasourceConfig(name=FLIGHTS_DS, type="duckdb", database=str(database)))
    async with owned_query_engine(storage=storage) as engine:
        entries = await engine.validate_datasets(data_source=FLIGHTS_DS)
        with pytest.warns(Warning, match=FLIGHTS_MAPPING):
            await engine.apply_drift_deletes(entries)

    reopened = make_storage(kind, tmp_path)
    flight = await reopened.get_dataset("FLIGHT", FLIGHTS_DS)
    assert flight is not None
    assert "dep_delay" not in [f.name for f in flight.fields]
    assert await reopened.list_ontology_mappings(FLIGHTS_DS) == [(FLIGHTS_DS, FLIGHTS_MAPPING)]
    problems = await reopened.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS)
    assert any("dep_delay" in p for p in problems), problems

    loaded, errors = await reopened.load_ontology_mappings(data_source=FLIGHTS_DS)
    assert FLIGHTS_MAPPING not in [m.name for m in loaded]
    assert [(e.kind, e.name, e.data_source) for e in errors] == [("ontology_mapping", FLIGHTS_MAPPING, FLIGHTS_DS)]
    assert "dep_delay" in str(errors[0])

    with raises_refusal("OntologyMappingInvalidError"):
        await reopened.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                                  text=example("flights.mapping.yaml"))
    assert await reopened.delete_ontology_mapping(FLIGHTS_MAPPING, FLIGHTS_DS) is True


@pytest.mark.parametrize("kind", STORAGES)
async def test_a_reingest_leaves_a_mapping_resolving_and_unwarned(tmp_path: Path, kind: str) -> None:
    """Ingest only adds and widens: a re-ingest after a column change keeps the mapping resolving."""
    database = tmp_path / "shop.duckdb"
    with contextlib.closing(duckdb.connect(str(database))) as con:
        con.execute("CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id VARCHAR, amount DOUBLE)")
        con.execute("CREATE TABLE customers (customer_id VARCHAR PRIMARY KEY)")
    config = DatasourceConfig(name="shop", type="duckdb", database=str(database))
    storage = await empty_store(kind, tmp_path)
    async with owned_query_engine(storage=storage, datasources=[config]) as engine:
        await engine.ingest_idempotent(config)
    await storage.save_ontology_text(name="shop", text=SHOP_ONTOLOGY)
    await storage.save_ontology_mapping_text(data_source="shop", name="shop_mapping",
                                             text=order_mapping(semantic_model="shop"))
    with contextlib.closing(duckdb.connect(str(database))) as con:
        con.execute("ALTER TABLE orders ALTER COLUMN amount TYPE VARCHAR")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        async with owned_query_engine(storage=storage, datasources=[config]) as engine:
            await engine.ingest_idempotent(config)
    assert not [w for w in caught if "shop_mapping" in str(w.message)]
    assert await storage.ontology_mapping_problems("shop_mapping", "shop") == []


async def test_a_resolving_mapping_loads(tmp_path: Path) -> None:
    storage = await flights_store("yaml", tmp_path)
    loaded, errors = await storage.load_ontology_mappings(data_source=FLIGHTS_DS)
    assert [m.name for m in loaded] == [FLIGHTS_MAPPING]
    assert errors == []


async def test_an_empty_store_lists_nothing(tmp_path: Path) -> None:
    storage = await empty_store("sqlite", tmp_path)
    assert await storage.list_ontologies() == []
    assert await storage.list_ontology_mappings() == []
    assert await storage.get_ontology_text(FLIGHTS) is None
