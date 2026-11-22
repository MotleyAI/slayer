"""pytest plugin for the Ossie corpus harvest, loaded into upstream's validator test run by `ossie_schema_drift.py`.

It wraps ``validate_document`` of every validate module a collected test module holds as ``_VALIDATE`` (and of a
top-level ``validate`` module), captures each flat semantic-model document passed to it, and at session end writes
those a pristine copy of the validator accepts without errors to ``$OSSIE_HARVEST_OUT/<sha256-12>.yaml``.
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

import pytest
import yaml

ENV_OUT = "OSSIE_HARVEST_OUT"
ENV_VALIDATE = "OSSIE_HARVEST_VALIDATE"
WRAPPED = "validate_document"
_MARK = "_ossie_harvest_wrapped"

_captured: list[dict[str, Any]] = []


def is_semantic_model(data: object) -> bool:
    """A flat semantic-model document: a mapping with root ``datasets``."""
    return isinstance(data, dict) and "datasets" in data


def wrap(module: ModuleType, sink: list[dict[str, Any]]) -> bool:
    """Make ``module.validate_document`` copy each semantic-model document it is given into ``sink``."""
    original = getattr(module, WRAPPED, None)
    if not callable(original) or getattr(original, _MARK, False):
        return False

    def capturing(data: Any, *args: Any, **kwargs: Any) -> Any:
        if is_semantic_model(data):
            try:
                sink.append(copy.deepcopy(data))
            except Exception:  # noqa: BLE001, S110 — an uncopyable document is not harvestable
                pass
        return original(data, *args, **kwargs)

    setattr(capturing, _MARK, True)
    setattr(module, WRAPPED, capturing)
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
    wrapped = sum(wrap(module._VALIDATE, _captured) for module in modules.values() if hasattr(module, "_VALIDATE"))
    if "validate" in sys.modules:
        wrapped += wrap(sys.modules["validate"], _captured)
    print(f"\nossie-harvest: wrapped {wrapped} validate module(s)")


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    out, validate = os.environ.get(ENV_OUT), os.environ.get(ENV_VALIDATE)
    if not out or not validate:
        return
    texts = harvest_texts(_captured, accepted=full_check(load_validator(Path(validate))))
    write_harvest(texts, Path(out))
    print(f"\nossie-harvest: {len(_captured)} captured, {len(texts)} written to {out}")
