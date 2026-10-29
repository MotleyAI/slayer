"""The vendored upstream Ossie schema: fixture validity, OSI model coverage, the shipped example."""

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import jsonschema
import pytest
import yaml
from pydantic import BaseModel

from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import (
    OSIAIContextObject,
    OSICustomExtension,
    OSIDataset,
    OSIDialectExpression,
    OSIDimension,
    OSIDocument,
    OSIExpression,
    OSIField,
    OSIMetric,
    OSIRelationship,
)
from slayer.osi.parser import parse_osi_path
from tests._engine_helpers import disposable_engine
from tests._osi_helpers import FIXTURES, SCHEMA_PATH, entries

EXAMPLE_DIR = Path(__file__).parent.parent / "docs" / "examples" / "13_osi_import"
EXAMPLE = EXAMPLE_DIR / "shop.osi.yaml"
CURRENT_FIXTURES = [FIXTURES / "current_shop.yaml"]
LEGACY_FIXTURES = [FIXTURES / name for name in
                   ("shop.yaml", "shop.json", "aicontext_string.yaml", "unknown_version.yaml")]

# Schema $defs → the OSI model that parses them; the root object parses as OSIDocument.
_MODEL_FOR_DEF: dict[str, type[BaseModel]] = {
    "SemanticModel": OSIDocument,
    "Dataset": OSIDataset,
    "Field": OSIField,
    "Relationship": OSIRelationship,
    "Metric": OSIMetric,
    "Expression": OSIExpression,
    "DialectExpression": OSIDialectExpression,
    "Dimension": OSIDimension,
    "CustomExtension": OSICustomExtension,
    "AIContext": OSIAIContextObject,
}
# $defs that are scalar value types, not objects.
_SCALAR_DEFS = frozenset({"Dialect", "Vendor", "DataType"})
# (def, property) pairs the importer knowingly does not model.
_IGNORED_PROPERTIES: frozenset[tuple[str, str]] = frozenset()


def _schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _load(path: Path) -> Any:
    text = path.read_text(encoding="utf-8")
    return json.loads(text) if path.suffix == ".json" else yaml.safe_load(text)


def _validator() -> jsonschema.Draft202012Validator:
    return jsonschema.Draft202012Validator(_schema())


def _object_properties(definition: dict[str, Any]) -> set[str]:
    if "properties" in definition:
        return set(definition["properties"])
    return {p for branch in definition.get("oneOf", []) for p in branch.get("properties", {})}


def _model_keys(model: type[BaseModel]) -> set[str]:
    return {info.alias or name for name, info in model.model_fields.items()}


@pytest.mark.parametrize("path", [*CURRENT_FIXTURES, EXAMPLE], ids=lambda p: p.name)
def test_current_spec_document_validates(path: Path) -> None:
    _validator().validate(_load(path))


@pytest.mark.parametrize("path", LEGACY_FIXTURES, ids=lambda p: p.name)
def test_legacy_fixture_is_outside_the_current_schema(path: Path) -> None:
    assert not _validator().is_valid(_load(path))


def test_every_schema_def_is_classified() -> None:
    defs = set(_schema()["$defs"])
    assert defs == set(_MODEL_FOR_DEF) | _SCALAR_DEFS


@pytest.mark.parametrize("def_name", sorted(_MODEL_FOR_DEF))
def test_schema_properties_are_modelled(def_name: str) -> None:
    properties = _object_properties(_schema()["$defs"][def_name])
    missing = properties - _model_keys(_MODEL_FOR_DEF[def_name]) - {
        prop for d, prop in _IGNORED_PROPERTIES if d == def_name}
    assert not missing, f"{def_name}: unmodelled schema properties {sorted(missing)}"


def test_root_properties_are_modelled() -> None:
    missing = set(_schema()["properties"]) - _model_keys(OSIDocument)
    assert not missing, f"document root: unmodelled schema properties {sorted(missing)}"


def _example_setup() -> ModuleType:
    spec = importlib.util.spec_from_file_location("setup_osi", EXAMPLE_DIR / "setup_osi.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_example_validates_and_imports_without_unconverted_entries(tmp_path: Path) -> None:
    _validator().validate(_load(EXAMPLE))
    db = _example_setup().build_shop_duckdb(tmp_path / "shop.duckdb")
    with disposable_engine(f"duckdb:///{db}") as engine:
        result = OsiToSlayerConverter(documents=parse_osi_path(EXAMPLE), data_source="shop_osi",
                                      sa_engine=engine, target_dialect="duckdb").convert()
    assert result.models
    assert not [e for e in entries(result) if e.severity == "unconverted"]
