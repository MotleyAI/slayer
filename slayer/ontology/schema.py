"""Validation against the vendored upstream Ossie schemas, every reference resolved from the package."""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from referencing import Registry, Resource

if TYPE_CHECKING:
    from jsonschema import Draft202012Validator
    from jsonschema.exceptions import ValidationError

SchemaKind = Literal["ontology", "mapping"]

_SCHEMAS = Path(__file__).parent / "schemas"
_FILES = {"core-spec/ossie-schema.json": "ossie-schema.json", "ontology/ontology.json": "ontology.json",
          "ontology/mapping.json": "mapping.json"}
_BASES = ("https://raw.githubusercontent.com/apache/ossie/main/", "https://github.com/apache/ossie/")


@cache
def _schema(file: str) -> dict[str, Any]:
    return json.loads((_SCHEMAS / file).read_text(encoding="utf-8"))


@cache
def _validator(kind: SchemaKind) -> Draft202012Validator:
    from jsonschema import Draft202012Validator  # noqa: PLC0415  # ALLOW(import-not-top): optional heavy dependency; a top-level import costs ~0.9s on every CLI/MCP start
    registry = Registry().with_resources(
        (base + upstream, Resource.from_contents(_schema(file))) for upstream, file in _FILES.items() for base in _BASES)
    return Draft202012Validator(_schema(f"{kind}.json"), registry=registry)


def _message(error: ValidationError) -> str:
    path = " -> ".join(str(p) for p in error.absolute_path) or "(root)"
    return f"{path}: {error.message}"


def schema_errors(data: Any, *, kind: SchemaKind) -> list[str]:
    """Every error of ``data`` against the vendored ``ontology.json`` or ``mapping.json``, offline."""
    return [_message(e) for e in sorted(_validator(kind).iter_errors(data), key=lambda e: list(map(str, e.absolute_path)))]
