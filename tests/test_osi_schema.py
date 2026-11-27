"""The vendored upstream Ossie schema: fixture validity, OSI model coverage, the shipped example."""

import json
from pathlib import Path
from typing import Any

import jsonschema
import pytest
import yaml
from pydantic import BaseModel

from slayer.osi.models import (
    OSIDataset,
    OSIDimension,
    OSIDocument,
    OSIField,
    OSIMetric,
    OSIRelationship,
)
from slayer.core.ossie_content import OSIAIContextObject
from slayer.core.ossie_expression import OSINonportableExtension
from slayer.core.ossie_expression import OSIExpression
from slayer.core.ossie_expression import OSIDialectExpression
from tests._osi_helpers import FIXTURES, SCHEMA_PATH

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
    "NonportableExtension": OSINonportableExtension,
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


def _dataset_keys(dataset: type[BaseModel]) -> set[str]:
    return {info.alias or name for name, info in dataset.model_fields.items()}


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
    missing = properties - _dataset_keys(_MODEL_FOR_DEF[def_name]) - {
        prop for d, prop in _IGNORED_PROPERTIES if d == def_name}
    assert not missing, f"{def_name}: unmodelled schema properties {sorted(missing)}"


def test_root_properties_are_modelled() -> None:
    missing = set(_schema()["properties"]) - _dataset_keys(OSIDocument)
    assert not missing, f"document root: unmodelled schema properties {sorted(missing)}"

