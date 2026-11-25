"""A rewrite of stored input inside a model validator counts as an upgrade change; a filled default does not.

Spec: models/stored-upgrade › A load warns when a document needs upgrading.
"""

from __future__ import annotations

import asyncio
import typing
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import BaseModel

import slayer.core.warnings as slayer_warnings
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.stored_rewrites import collecting_rewrites
from slayer.memories.models import Memory
from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.upgrade import upgrade_store
from tests._granularity_rename_fixtures import BACKENDS, raw_documents, reopen, write_raw_store

ROOTS: tuple[type[BaseModel], ...] = (SlayerDataset, SlayerQuery, Memory, DatasourceConfig)

#: A validator that rewrites stored input → an input it rewrites, validated through its model.
REWRITING: dict[str, tuple[type[BaseModel], dict[str, Any]]] = {
    "slayer.core.models.DatasourceConfig._apply_schema_migrations_and_aliases": (
        DatasourceConfig, {"name": "pg", "type": "postgres", "host": "localhost", "user": "alice"}),
}

#: A validator that rewrites no stored input → why.
NOT_REWRITING: dict[str, str] = {
    "slayer.core.format.NumberFormat._default_symbol": "fills a default",
    "slayer.core.granularity.CustomGranularity._default_origin": "fills a default",
    "slayer.core.query.QueryInput._refuse_unknown_keys": "refuses",
    "slayer.core.query.DatasetExtension._refuse_fields": "refuses",
    "slayer.core.query.OrderItem._capture_raw_expression": "derives a field it never stores",
    "slayer.core.query.TimeDimension._reject_placeholder_names": "refuses",
    "slayer.memories.models.Memory._apply_schema_migrations": "versioned migration, counted by the version",
}


def _models(annotation: Any, seen: set[type[BaseModel]]) -> None:
    if typing.get_origin(annotation) is not None:
        for arg in typing.get_args(annotation):
            _models(arg, seen)
    elif isinstance(annotation, type) and issubclass(annotation, BaseModel) and annotation not in seen:
        seen.add(annotation)
        for field in annotation.model_fields.values():
            _models(field.annotation, seen)


def _before_validators() -> set[str]:
    seen: set[type[BaseModel]] = set()
    for root in ROOTS:
        _models(root, seen)
    out: set[str] = set()
    for model in seen:
        for decorator in model.__pydantic_decorators__.model_validators.values():
            if decorator.info.mode == "before":
                func: Callable[..., Any] = getattr(decorator.func, "__func__", decorator.func)
                out.add(f"{func.__module__}.{func.__qualname__}")
    return out


def test_every_before_validator_is_classified() -> None:
    assert _before_validators() == set(REWRITING) | set(NOT_REWRITING)


@pytest.mark.parametrize("validator", list(REWRITING))
def test_a_rewriting_validator_marks(validator: str) -> None:
    model, data = REWRITING[validator]
    with collecting_rewrites() as collected:
        model.model_validate(data)
    assert collected.rewritten


def test_a_default_fill_does_not_mark() -> None:
    with collecting_rewrites() as collected:
        DatasourceConfig.model_validate({"name": "s", "type": "sqlite", "database": ":memory:",
                                         "granularities": [{"name": "fortnight", "base": "week", "multiple": 2}]})
    assert not collected.rewritten


def _upgrade_warnings(record: list[warnings.WarningMessage]) -> list[Any]:
    carrier = slayer_warnings.SlayerStoredUpgradeNeededWarning
    return [getattr(w.message, "payload") for w in record if isinstance(w.message, carrier)]


def _current_datasource(**fields: Any) -> dict[str, Any]:
    return {"version": CURRENT_VERSIONS["DatasourceConfig"], **fields}


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_validator_level_rewrite_warns_and_is_persisted(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / backend)
    doc = _current_datasource(name="pg", type="postgres", host="localhost", database="shop", user="alice")
    storage = asyncio.run(write_raw_store(backend=backend, base=base, datasources=[doc]))
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        loaded = asyncio.run(storage.get_datasource("pg"))
    assert loaded is not None
    assert loaded.username == "alice"
    assert [(p.document_kind, p.name) for p in _upgrade_warnings(record)] == [("datasource", "pg")]
    assert asyncio.run(upgrade_store(reopen(backend=backend, base=base))).complete
    stored = yaml.safe_load(raw_documents(backend=backend, base=base)["datasources/pg"])  # JSON is YAML
    assert stored["username"] == "alice"
    assert "user" not in stored


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_defaulted_key_does_not_warn(backend: str, tmp_path: Path) -> None:
    base = str(tmp_path / backend)
    doc = _current_datasource(name="s", type="sqlite", database=":memory:",
                              granularities=[{"name": "fortnight", "base": "week", "multiple": 2}])
    storage = asyncio.run(write_raw_store(backend=backend, base=base, datasources=[doc]))
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        assert asyncio.run(storage.get_datasource("s")) is not None
    assert _upgrade_warnings(record) == []
