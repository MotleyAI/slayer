"""``import-osi`` imports ontology and mapping documents, in dependency order, and upgrades embedded mappings.

Spec: importers/osi-import › Upstream examples import (Ontology examples); Invalid documents are refused with every
reason (Ontology document); Ontology and mapping documents import with their references; Embedded ontology mappings
are upgraded.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests._cli_inprocess import run_cli_in_process
from tests._dev2243_fixtures import STORAGES, make_storage
from tests._dev2474_fixtures import (
    FLIGHTS, FLIGHTS_DS, FLIGHTS_MAPPING, FLIGHTS_MODEL, SAP, SAP_DS, SAP_MAPPING, SAP_MODEL, component, dump, edited,
    empty_store, example, load, offline, refusal_text, run_import, write,
)

FLIGHTS_TRIO = ("flights.ontology.yaml", "flights.semantic_model.yaml", "flights.mapping.yaml")
SAP_TRIO = ("sap_business_partner.ontology.yaml", "sap_business_partner.semantic_model.yaml",
            "sap_business_partner.mapping.yaml")


def folder(base: Path, files: dict[str, str]) -> Path:
    for name, text in files.items():
        write(base / name, text)
    return base


def upgrades(result: Any) -> list[Any]:
    return [e for e in result.entries if e.kind == "upgraded"]


def bad_ontology() -> str:
    """``flights.ontology.yaml`` with a concept extending an undeclared one."""
    return edited(example("flights.ontology.yaml"),
                  lambda d: d["ontology"].append({"concept": "Jet", "type": "EntityType", "extends": ["Plane"]}))


# --------------------------------------------------------------------------- #
# Upstream examples import
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_ontology_examples(tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: Ontology examples."""
    offline(monkeypatch)
    runs: dict[str, tuple[str, tuple[str, ...]]] = {
        "split": (FLIGHTS_DS, FLIGHTS_TRIO), "embedded": (FLIGHTS_DS, ("flights.yaml",)),
        "sap": (SAP_DS, SAP_TRIO), "foaf": ("anywhere", ("foaf_owl_import.yaml",)),
    }
    results: dict[str, Any] = {}
    stores: dict[str, Any] = {}
    for run, (data_source, names) in runs.items():
        storage = await empty_store(kind, tmp_path / run, data_source)
        source = folder(tmp_path / run / "in", {name: example(name) for name in names})
        results[run] = await run_import(storage, source, data_source=data_source)
        stores[run] = storage
        assert results[run].refused == [], (run, results[run].refused)

    for run in ("split", "embedded"):
        storage = stores[run]
        assert await storage.list_ontologies() == [FLIGHTS]
        assert await storage.list_ontology_mappings() == [(FLIGHTS_DS, FLIGHTS_MAPPING)]
        assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, FLIGHTS_DS) == []
    assert await stores["sap"].list_ontologies() == [SAP]
    assert await stores["sap"].list_ontology_mappings() == [(SAP_DS, SAP_MAPPING)]
    assert await stores["sap"].ontology_mapping_problems(SAP_MAPPING, SAP_DS) == []
    assert await stores["foaf"].list_ontologies() == ["foaf"]
    assert await stores["foaf"].list_ontology_mappings() == []

    [upgrade] = upgrades(results["embedded"])
    assert "flights.yaml" in f"{upgrade.document or ''} {upgrade.message}"
    assert upgrades(results["split"]) == []


async def test_ontology_document(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: Ontology document (a root that is none of the three kinds)."""
    offline(monkeypatch)
    storage = await empty_store("yaml", tmp_path, FLIGHTS_DS)
    source = folder(tmp_path / "in", {"bad.yaml": dump({"version": "0.3.0.dev0", "name": "x", "foo": 1}),
                                      "good.yaml": example("flights.ontology.yaml")})
    result = await run_import(storage, source, data_source=FLIGHTS_DS)
    reason = refusal_text(result, "bad.yaml").lower()
    assert "semantic-model" in reason
    assert "ontology" in reason
    assert "mapping" in reason
    assert await storage.list_ontologies() == [FLIGHTS]


async def test_a_file_too_deeply_nested_to_parse_is_refused_alone(tmp_path: Path,
                                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    """A parse that overflows the recursion limit refuses its file; the import goes on."""
    offline(monkeypatch)
    storage = await empty_store("yaml", tmp_path, FLIGHTS_DS)
    source = folder(tmp_path / "in", {"deep.yaml": "[" * 5000, "good.yaml": example("flights.ontology.yaml")})
    result = await run_import(storage, source, data_source=FLIGHTS_DS)
    assert refusal_text(result, "deep.yaml")
    assert await storage.list_ontologies() == [FLIGHTS]


@pytest.mark.parametrize("document", [
    pytest.param({"version": "0.3.0.dev0", "name": "m", "concept_mappings": []}, id="mapping-without-references"),
    pytest.param({"version": "0.3.0.dev0", "name": "m", "ontology_ref": {"name": "o"}, "concept_mappings": []},
                 id="mapping-without-semantic-model-reference"),
])
async def test_a_root_short_of_the_mapping_keys_is_no_document(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                document: dict[str, Any]) -> None:
    offline(monkeypatch)
    storage = await empty_store("yaml", tmp_path, FLIGHTS_DS)
    result = await run_import(storage, write(tmp_path / "bad.yaml", dump(document)), data_source=FLIGHTS_DS)
    assert refusal_text(result, "bad.yaml")
    assert await storage.list_ontology_mappings() == []


# --------------------------------------------------------------------------- #
# Import with references
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_file_order_does_not_matter(tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: File order does not matter."""
    offline(monkeypatch)
    storage = await empty_store(kind, tmp_path, "ds")
    source = folder(tmp_path / "in", {"a.mapping.yaml": example("flights.mapping.yaml"),
                                      "b.semantic_model.yaml": example("flights.semantic_model.yaml"),
                                      "c.ontology.yaml": example("flights.ontology.yaml")})
    result = await run_import(storage, source, data_source="ds")
    assert result.refused == []
    assert await storage.list_ontologies() == [FLIGHTS]
    assert await storage.list_semantic_models("ds") == [("ds", FLIGHTS_MODEL)]
    assert await storage.list_ontology_mappings() == [("ds", FLIGHTS_MAPPING)]
    assert await storage.ontology_mapping_problems(FLIGHTS_MAPPING, "ds") == []


async def test_a_refused_ontology_refuses_its_mapping(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: A refused ontology refuses its mapping."""
    offline(monkeypatch)
    storage = await empty_store("yaml", tmp_path, FLIGHTS_DS)
    source = folder(tmp_path / "in", {"o.yaml": bad_ontology(), "s.yaml": example("flights.semantic_model.yaml"),
                                      "m.yaml": example("flights.mapping.yaml")})
    result = await run_import(storage, source, data_source=FLIGHTS_DS)
    assert "Plane" in refusal_text(result, "o.yaml")
    assert FLIGHTS in refusal_text(result, "m.yaml")
    assert await storage.list_ontologies() == []
    assert await storage.list_ontology_mappings() == []
    assert await storage.list_semantic_models(FLIGHTS_DS) == [(FLIGHTS_DS, FLIGHTS_MODEL)]


async def test_a_refused_import_exits_non_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    offline(monkeypatch)
    await empty_store("yaml", tmp_path, FLIGHTS_DS)
    source = folder(tmp_path / "in", {"o.yaml": bad_ontology(), "m.yaml": example("flights.mapping.yaml")})
    result = run_cli_in_process(["import-osi", str(source), "--datasource", FLIGHTS_DS,
                                 "--storage", str(tmp_path / "store")])
    assert result.returncode != 0
    stored = make_storage("yaml", tmp_path)
    assert await stored.list_ontologies() == []


async def test_a_clean_cli_import_exits_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    offline(monkeypatch)
    await empty_store("yaml", tmp_path, FLIGHTS_DS)
    source = folder(tmp_path / "in", {name: example(name) for name in FLIGHTS_TRIO})
    result = run_cli_in_process(["import-osi", str(source), "--datasource", FLIGHTS_DS,
                                 "--storage", str(tmp_path / "store")])
    assert result.returncode == 0, result.stderr
    stored = make_storage("yaml", tmp_path)
    assert await stored.list_ontology_mappings() == [(FLIGHTS_DS, FLIGHTS_MAPPING)]


async def test_an_ontology_is_global_whatever_the_datasource(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    offline(monkeypatch)
    storage = await empty_store("yaml", tmp_path, "elsewhere")
    result = await run_import(storage, write(tmp_path / "o.yaml", example("flights.ontology.yaml")),
                              data_source="elsewhere")
    assert result.refused == []
    assert await storage.list_ontologies() == [FLIGHTS]
    assert await storage.get_ontology_text(FLIGHTS) == example("flights.ontology.yaml")


async def test_reimporting_replaces(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    offline(monkeypatch)
    storage = await empty_store("yaml", tmp_path, FLIGHTS_DS)
    await run_import(storage, folder(tmp_path / "first", {n: example(n) for n in FLIGHTS_TRIO}),
                     data_source=FLIGHTS_DS)
    ontology = example("flights.ontology.yaml").replace("Ontology of flights", "Ontology of all flights", 1)
    mapping = example("flights.mapping.yaml").replace("Maps the Flights", "Maps all the Flights", 1)
    result = await run_import(storage, folder(tmp_path / "second", {"o.yaml": ontology, "m.yaml": mapping}),
                              data_source=FLIGHTS_DS)
    assert result.refused == []
    assert await storage.get_ontology_text(FLIGHTS) == ontology
    assert await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) == mapping


async def test_a_reimport_breaking_a_mapping_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    offline(monkeypatch)
    storage = await empty_store("yaml", tmp_path, FLIGHTS_DS)
    await run_import(storage, folder(tmp_path / "first", {n: example(n) for n in FLIGHTS_TRIO}),
                     data_source=FLIGHTS_DS)
    without_delay = edited(example("flights.ontology.yaml"), lambda d: d["ontology"].remove(component(d, "Delay")))
    result = await run_import(storage, write(tmp_path / "o.yaml", without_delay), data_source=FLIGHTS_DS)
    assert FLIGHTS_MAPPING in refusal_text(result, "o.yaml")
    assert await storage.get_ontology_text(FLIGHTS) == example("flights.ontology.yaml")


# --------------------------------------------------------------------------- #
# Embedded ontology mappings are upgraded
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("kind", STORAGES)
async def test_flights_yaml_is_upgraded(tmp_path: Path, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec scenario: flights.yaml is upgraded."""
    offline(monkeypatch)
    storage = await empty_store(kind, tmp_path, FLIGHTS_DS)
    result = await run_import(storage, write(tmp_path / "flights.yaml", example("flights.yaml")),
                              data_source=FLIGHTS_DS)
    assert result.refused == []
    ontology = load(await storage.get_ontology_text(FLIGHTS) or "")
    assert "ontology_mappings" not in ontology
    assert ontology == load(example("flights.ontology.yaml"))
    assert await storage.list_semantic_models(FLIGHTS_DS) == [(FLIGHTS_DS, FLIGHTS_MODEL)]
    mapping = load(await storage.get_ontology_mapping_text(FLIGHTS_MAPPING, FLIGHTS_DS) or "")
    assert mapping["ontology_ref"]["name"] == FLIGHTS
    assert mapping["semantic_model_ref"]["name"] == FLIGHTS_MODEL
    assert mapping["concept_mappings"] == load(example("flights.mapping.yaml"))["concept_mappings"]
    [upgrade] = upgrades(result)
    assert "flights.yaml" in f"{upgrade.document or ''} {upgrade.message}"


@pytest.mark.parametrize("change", [
    pytest.param(lambda d: d["ontology_mappings"][0]["semantic_model"]["datasets"].append(
        d["ontology_mappings"][0]["semantic_model"]["datasets"][0]), id="inline-model-refused"),
    pytest.param(lambda d: d["ontology_mappings"][0]["concept_mappings"][0].update(concept="Ordr"),
                 id="mapping-refused"),
])
async def test_a_refused_embedded_part_refuses_the_whole_document(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                  change: Any) -> None:
    offline(monkeypatch)
    storage = await empty_store("yaml", tmp_path, FLIGHTS_DS)
    result = await run_import(storage, write(tmp_path / "flights.yaml", edited(example("flights.yaml"), change)),
                              data_source=FLIGHTS_DS)
    assert refusal_text(result, "flights.yaml")
    assert await storage.list_ontologies() == []
    assert await storage.list_semantic_models(FLIGHTS_DS) == []
    assert await storage.list_ontology_mappings() == []


async def test_sap_semantic_model_imports_under_its_own_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    offline(monkeypatch)
    storage = await empty_store("yaml", tmp_path, SAP_DS)
    result = await run_import(storage, folder(tmp_path / "in", {n: example(n) for n in SAP_TRIO}), data_source=SAP_DS)
    assert result.refused == []
    assert await storage.list_semantic_models(SAP_DS) == [(SAP_DS, SAP_MODEL)]
