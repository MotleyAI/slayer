"""Shared documents, stores and builders for the Ossie Layer 4 ontology and mapping tests.

New storage methods and errors are reached at call time, so a name the implementation has not added yet fails the
one test that needs it rather than the whole module's collection.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.access_filtered import AccessFilteredStorage
from slayer.storage.base import StorageBackend
from tests._dev2243_fixtures import CORPUS, error, importer_module, make_storage

EXAMPLES = CORPUS / "examples"
HARVESTED_CASES = CORPUS / "harvested" / "ontology"
FLIGHTS_DS = "flights"
SAP_DS = "sap"
FLIGHTS = "Flights"
FLIGHTS_MODEL = "Flights semantic model"
FLIGHTS_MAPPING = "flights_mapping"
SAP = "sap_business_partner"
SAP_MODEL = "sap_customer_sales"
SAP_MAPPING = "sap_business_partner_mapping"
#: The Flight concept mapping's departure_delay child in ``flights.mapping.yaml``.
DEP_DELAY_PATH = "concept_mappings -> 8 -> link_mappings -> 2 -> children -> 1 -> object_mapping"


def example(name: str) -> str:
    return (EXAMPLES / name).read_text(encoding="utf-8")


def load(text: str) -> Any:
    return yaml.safe_load(text)


def dump(document: Any) -> str:
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=True)


def edited(text: str, change: Callable[[Any], None]) -> str:
    """``text`` parsed, changed in place by ``change`` and dumped."""
    document = load(text)
    change(document)
    return dump(document)


def concept_mapping(document: dict[str, Any], concept: str) -> dict[str, Any]:
    [hit] = [m for m in document["concept_mappings"] if m["concept"] == concept]
    return hit


def component(document: dict[str, Any], concept: str) -> dict[str, Any]:
    [hit] = [c for c in document["ontology"] if c["concept"] == concept]
    return hit


def refusal(exc: BaseException) -> str:
    """Everything a refusal says: its message and, when it carries them, its reasons."""
    return "\n".join([str(exc), *map(str, getattr(exc, "reasons", None) or [])])


# --------------------------------------------------------------------------- #
# Small ontologies and mappings
# --------------------------------------------------------------------------- #
def ontology_text(*components: dict[str, Any], name: str = "shop", **top: Any) -> str:
    return dump({"version": "0.3.0.dev0", "name": name, **top, "ontology": list(components)})


def concept(name: str, kind: str = "EntityType", **extra: Any) -> dict[str, Any]:
    return {"concept": name, "type": kind, **extra}


def rel(name: str, *roles: str | dict[str, Any], **extra: Any) -> dict[str, Any]:
    """A relationship whose further roles are concept names or role objects, verbalized unless ``extra`` says how."""
    role_objects = [r if isinstance(r, dict) else {"concept": r} for r in roles]
    placeholders = " ".join(f"{{{r['concept']}}}" for r in role_objects)
    out: dict[str, Any] = {"name": name, "verbalizes": [f"{{_}} {name} {placeholders}".rstrip()], **extra}
    if role_objects:
        out["roles"] = role_objects
    return out


def mapping_text(*concept_mappings: dict[str, Any], name: str = "shop_mapping", ontology: str = "shop",
                 semantic_model: str = "shop") -> str:
    return dump({"version": "0.3.0.dev0", "name": name, "ontology_ref": {"name": ontology},
                 "semantic_model_ref": {"name": semantic_model}, "concept_mappings": list(concept_mappings)})


#: Orders identified by ``nr`` (an ``OrderNr`` reaching Integer), each placed by one Customer.
SHOP_ONTOLOGY = ontology_text(
    concept("OrderNr", "ValueType", extends=["Integer"]),
    concept("Amount", "ValueType", extends=["Decimal"]),
    concept("CustomerId", "ValueType", extends=["String"]),
    concept("Customer", identify_by=["id"], relationships=[rel("id", "CustomerId")]),
    concept("Order", identify_by=["nr"], relationships=[rel("nr", "OrderNr"), rel("amount", "Amount"),
                                                         rel("placed_by", "Customer")]),
)


def shop_datasets(*, amount_type: str | None = "Float") -> list[dict[str, Any]]:
    """``orders`` (order_id, customer_id, amount) and ``customers`` (customer_id), typed unless told otherwise."""
    def typed(name: str, datatype: str | None) -> dict[str, Any]:
        out: dict[str, Any] = {"name": name, "expression": {"sql": name}}
        if datatype is not None:
            out["datatype"] = datatype
        return out
    return [
        {"name": "orders", "source": "orders", "primary_key": ["order_id"],
         "fields": [typed("order_id", "Integer"), typed("customer_id", "String"), typed("amount", amount_type)]},
        {"name": "customers", "source": "customers", "primary_key": ["customer_id"],
         "fields": [typed("customer_id", "String")]},
    ]


def shop_model_text(*, name: str = "shop_model", amount_type: str | None = "Float") -> str:
    return dump({"version": "0.3.0.dev0", "name": name, "datasets": shop_datasets(amount_type=amount_type)})


def order_mapping(*, amount: str = "orders.amount", **refs: str) -> str:
    """Order by its ``nr``, with its ``amount``."""
    return mapping_text(
        {"concept": "Order",
         "object_mappings": [{"referent_mappings": [{"relationship": "nr", "expression": "orders.order_id"}]}],
         "link_mappings": [{"object_mapping": {"referent_mappings": [
             {"relationship": "nr", "expression": "orders.order_id"}]},
             "children": [{"relationship": "amount", "object_mapping": {"concept": "Amount", "expression": amount}}]}]},
        **refs,
    )


# --------------------------------------------------------------------------- #
# Stores
# --------------------------------------------------------------------------- #
def _offline_config(*, base: Path, name: str) -> DatasourceConfig:
    return DatasourceConfig(name=name, type="duckdb", database=str(base / "absent" / f"{name}.duckdb"))


def _no_database(_self: SlayerQueryEngine, _datasource: DatasourceConfig) -> Any:
    raise AssertionError("database accessed")


def offline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any database client construction fails."""
    monkeypatch.setattr(SlayerQueryEngine, "client_for", _no_database)


async def empty_store(kind: str, base: Path, *data_sources: str) -> StorageBackend:
    """A store holding ``data_sources``, each over a missing database."""
    base.mkdir(parents=True, exist_ok=True)
    storage = make_storage(kind, base)
    for name in data_sources:
        await storage.save_datasource(_offline_config(base=base, name=name))
    return storage


async def shop_store(kind: str, base: Path, *, amount_type: str | None = "Float", ontology: str = SHOP_ONTOLOGY,
                     standalone: bool = False) -> StorageBackend:
    """Datasource ``shop`` holding ontology ``shop`` and, as document ``shop_model`` or (``standalone``) as standalone
    datasets, ``orders`` and ``customers``."""
    storage = await empty_store(kind, base, "shop")
    await storage.save_ontology_text(name="shop", text=ontology)
    if standalone:
        for raw in shop_datasets(amount_type=amount_type):
            await storage.save_dataset(SlayerDataset.model_validate({
                "name": raw["name"], "sql_table": raw["source"], "data_source": "shop",
                "fields": [{"name": f["name"], "expression": f["name"], "primary_key": f["name"] in raw["primary_key"],
                            **({"datatype": f["datatype"]} if "datatype" in f else {})} for f in raw["fields"]]}))
    else:
        await storage.save_semantic_model_text(data_source="shop", name="shop_model",
                                               text=shop_model_text(amount_type=amount_type))
    return storage


async def flights_store(kind: str, base: Path, *, mapping: bool = True,
                        model: str | None = None) -> StorageBackend:
    """Datasource ``flights`` holding ontology ``Flights``, the flights semantic model and (``mapping``) the mapping."""
    storage = await empty_store(kind, base, FLIGHTS_DS)
    await storage.save_ontology_text(name=FLIGHTS, text=example("flights.ontology.yaml"))
    await storage.save_semantic_model_text(data_source=FLIGHTS_DS, name=FLIGHTS_MODEL,
                                           text=model or example("flights.semantic_model.yaml"))
    if mapping:
        await storage.save_ontology_mapping_text(data_source=FLIGHTS_DS, name=FLIGHTS_MAPPING,
                                                 text=example("flights.mapping.yaml"))
    return storage


async def sap_store(kind: str, base: Path, *, mapping: bool = True) -> StorageBackend:
    storage = await empty_store(kind, base, SAP_DS)
    await storage.save_ontology_text(name=SAP, text=example("sap_business_partner.ontology.yaml"))
    await storage.save_semantic_model_text(data_source=SAP_DS, name=SAP_MODEL,
                                           text=example("sap_business_partner.semantic_model.yaml"))
    if mapping:
        await storage.save_ontology_mapping_text(data_source=SAP_DS, name=SAP_MAPPING,
                                                 text=example("sap_business_partner.mapping.yaml"))
    return storage


def view(storage: StorageBackend, *tags: str) -> AccessFilteredStorage:
    """``storage`` as seen by a caller holding ``tags``."""
    return AccessFilteredStorage(storage, tags=frozenset(tags), bypass=False)


async def hide(storage: StorageBackend, dataset: str, *, data_source: str) -> None:
    """Tag ``dataset`` so a caller without the ``secret`` tag cannot see it."""
    found = await storage.get_dataset(dataset, data_source)
    assert found is not None, dataset
    await storage.save_dataset(found.model_copy(update={"access_tags": ["secret"]}))


# --------------------------------------------------------------------------- #
# Import
# --------------------------------------------------------------------------- #
async def run_import(storage: StorageBackend, path: Path, *, data_source: str) -> Any:
    return await importer_module().import_osi(storage=storage, path=path, data_source=data_source)


def refusal_text(result: Any, file_name: str) -> str:
    hits = [r for r in result.refused if Path(r.file).name == file_name]
    assert len(hits) == 1, result.refused
    return "\n".join(hits[0].reasons)


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


@contextlib.contextmanager
def raises_refusal(name: str) -> Generator[pytest.ExceptionInfo[Exception]]:
    """Expect the typed refusal ``slayer.core.errors.<name>``."""
    with pytest.raises(error(name)) as info:
        yield info
