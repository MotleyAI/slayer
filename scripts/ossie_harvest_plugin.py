"""pytest plugin for the Ossie corpus harvest, loaded into upstream's validator test run by `ossie_schema_drift.py`.

It wraps ``validate_document`` of every validate module a collected test module holds as ``_VALIDATE`` (and of a
top-level ``validate`` module), captures each flat semantic-model document passed to it, and at session end writes
those a pristine copy of the validator accepts without errors to ``$OSSIE_HARVEST_OUT/<sha256-12>.yaml``.

Ontology and mapping documents are captured as cases, accepted or not: each passed to ``validate_document`` (with the
text of every relative-path document its references name, read from ``base``) or validated against the root of
``ontology/ontology.json`` or ``ontology/mapping.json`` outside it. They go to
``$OSSIE_HARVEST_OUT/ontology/<sha256-12>.yaml`` as ``{document, files}``; the verdict is the consumer's to compute.
"""

import copy
import hashlib
import importlib.util
import json
import os
import sys
from collections.abc import Callable, Iterable
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import unquote, urlsplit

import pytest
import yaml
from jsonschema import Draft202012Validator

ENV_OUT = "OSSIE_HARVEST_OUT"
ENV_VALIDATE = "OSSIE_HARVEST_VALIDATE"
WRAPPED = "validate_document"
ONTOLOGY_DIR = "ontology"
REFERENCES = ("ontology_ref", "semantic_model_ref")
ONTOLOGY_SCHEMA_IDS = ("ontology/ontology.json", "ontology/mapping.json")
_MARK = "_ossie_harvest_wrapped"

_captured: list[dict[str, Any]] = []
_cases: list[dict[str, Any]] = []
_inside_validate_document = [0]


def is_semantic_model(data: object) -> bool:
    """A flat semantic-model document: a mapping with root ``datasets``."""
    return isinstance(data, dict) and "datasets" in data


def is_ontology_case(data: object) -> bool:
    """An ontology document (root ``ontology``) or a mapping document (root ``concept_mappings``)."""
    return isinstance(data, dict) and ("ontology" in data or "concept_mappings" in data)


def reference_path(iri: object) -> str | None:
    """The file path a relative-path reference ``iri`` names, as upstream follows it; None for any other iri."""
    if not isinstance(iri, str) or not iri:
        return None
    try:
        parts = urlsplit(iri)
    except ValueError:
        return None
    if parts.scheme or parts.netloc:
        return None
    return unquote(parts.path)


def referenced_files(data: dict[str, Any], base: object) -> dict[str, str] | None:
    """iri → text of each relative-path document a mapping references, read from ``base``; None when an iri is an
    absolute path (its location is the run's own, so the case is not reproducible)."""
    files: dict[str, str] = {}
    for key in REFERENCES:
        reference = data.get(key)
        iri = reference.get("iri") if isinstance(reference, dict) else None
        path = reference_path(iri)
        if path is None or not path:
            continue
        if Path(path).is_absolute():
            return None
        if not isinstance(base, Path):
            continue
        try:
            files[str(iri)] = (base / path).read_text(encoding="utf-8")
        except (OSError, UnicodeError, ValueError):
            continue
    return files


def capture_case(data: Any, base: object, cases: list[dict[str, Any]]) -> None:
    """Append ``{document, files}`` for an ontology or mapping document to ``cases``."""
    if not is_ontology_case(data):
        return
    files = referenced_files(data, base) if "concept_mappings" in data else {}
    if files is None:
        return
    try:
        cases.append({"document": copy.deepcopy(data), "files": files})
    except Exception:  # noqa: BLE001, S110 — an uncopyable document is not harvestable
        pass


def wrap(module: ModuleType, sink: list[dict[str, Any]], cases: list[dict[str, Any]] | None = None) -> bool:
    """Make ``module.validate_document`` copy each semantic-model document it is given into ``sink``, and each
    ontology or mapping document into ``cases``."""
    original = getattr(module, WRAPPED, None)
    if not callable(original) or getattr(original, _MARK, False):
        return False

    def capturing(data: Any, *args: Any, **kwargs: Any) -> Any:
        if is_semantic_model(data):
            try:
                sink.append(copy.deepcopy(data))
            except Exception:  # noqa: BLE001, S110 — an uncopyable document is not harvestable
                pass
        if cases is not None:
            capture_case(data, args[1] if len(args) > 1 else kwargs.get("base"), cases)
        _inside_validate_document[0] += 1
        try:
            return original(data, *args, **kwargs)
        finally:
            _inside_validate_document[0] -= 1

    setattr(capturing, _MARK, True)
    setattr(module, WRAPPED, capturing)
    return True


def is_ontology_schema(schema: object) -> bool:
    """The root of upstream's ontology or mapping schema."""
    schema_id = schema.get("$id") if isinstance(schema, dict) else None
    return isinstance(schema_id, str) and schema_id.endswith(ONTOLOGY_SCHEMA_IDS)


def wrap_schema_validator(validator_class: type, cases: list[dict[str, Any]]) -> bool:
    """Make ``validator_class.iter_errors`` capture each ontology or mapping document validated against the root
    of an ontology schema outside ``validate_document``."""
    original = getattr(validator_class, "iter_errors", None)
    if not callable(original) or getattr(original, _MARK, False):
        return False

    def capturing(self: Any, instance: Any, *args: Any, **kwargs: Any) -> Any:
        if not _inside_validate_document[0] and is_ontology_schema(getattr(self, "schema", None)):
            capture_case(instance, None, cases)
        return original(self, instance, *args, **kwargs)

    setattr(capturing, _MARK, True)
    setattr(validator_class, "iter_errors", capturing)
    return True


def load_validator(path: Path) -> ModuleType:
    """A private, never-monkeypatched copy of upstream ``validate.py``."""
    spec = importlib.util.spec_from_file_location("ossie_harvest_validate", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def full_check(validator: ModuleType) -> Callable[[dict[str, Any]], bool]:
    """Whether the validator's every check passes a document with no non-warning diagnostic."""
    schema = json.loads(Path(validator.default_schema_path()).read_text(encoding="utf-8"))

    def accepted(document: dict[str, Any]) -> bool:
        try:
            findings = validator.validate_document(copy.deepcopy(document), schema)
        except Exception:  # noqa: BLE001 — a check that raises does not accept the document
            return False
        return not any(not isinstance(f, validator.ValidationWarning) for f in findings)

    return accepted


def canonical_json(document: dict[str, Any]) -> str | None:
    """Key-sorted compact JSON, the dedup key; None for a document JSON cannot hold."""
    try:
        return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError):
        return None


def document_yaml(document: dict[str, Any]) -> str:
    return yaml.safe_dump(document, sort_keys=False, allow_unicode=True, default_flow_style=False, width=1000)


def harvest_texts(documents: Iterable[dict[str, Any]], *, accepted: Callable[[dict[str, Any]], bool]) -> dict[str, str]:
    """``<sha256-12>.yaml`` → YAML text of each accepted document, deduplicated by canonical JSON.

    Of documents equal up to key order, the lexicographically smallest text wins, so the output never depends on
    test order.
    """
    texts: dict[str, str] = {}
    for document in documents:
        key = canonical_json(document)
        if key is None or not accepted(document):
            continue
        name = f"{hashlib.sha256(key.encode('utf-8')).hexdigest()[:12]}.yaml"
        text = document_yaml(document)
        if name not in texts or text < texts[name]:
            texts[name] = text
    return dict(sorted(texts.items()))


def write_harvest(texts: dict[str, str], out: Path) -> None:
    """Replace every ``*.yaml`` in ``out`` with ``texts``."""
    out.mkdir(parents=True, exist_ok=True)
    for stale in out.glob("*.yaml"):
        stale.unlink()
    for name, text in texts.items():
        (out / name).write_text(text, encoding="utf-8")


def pytest_collection_finish(session: pytest.Session) -> None:
    found = (getattr(item, "module", None) for item in session.items)
    modules = {id(module): module for module in found if module is not None}
    wrapped = sum(wrap(module._VALIDATE, _captured, _cases) for module in modules.values()
                  if hasattr(module, "_VALIDATE"))
    if "validate" in sys.modules:
        wrapped += wrap(sys.modules["validate"], _captured, _cases)
    wrap_schema_validator(Draft202012Validator, _cases)
    print(f"\nossie-harvest: wrapped {wrapped} validate module(s)")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    out, validate = os.environ.get(ENV_OUT), os.environ.get(ENV_VALIDATE)
    if not out or not validate:
        return
    texts = harvest_texts(_captured, accepted=full_check(load_validator(Path(validate))))
    write_harvest(texts, Path(out))
    cases = harvest_texts(_cases, accepted=lambda case: True)
    write_harvest(cases, Path(out) / ONTOLOGY_DIR)
    print(f"\nossie-harvest: {len(_captured)} captured, {len(texts)} written to {out}; "
          f"{len(cases)} ontology case(s) written to {Path(out) / ONTOLOGY_DIR}")
