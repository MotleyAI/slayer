"""Structured warnings name their kind and payload fields in the vocabulary.

The stored documents below are older-version stored corpus: keep this file out of the vocabulary respelling pass.

Spec: compatibility/ossie-vocabulary › Warnings use the vocabulary.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import DatasourceConfig
import slayer.engine.query_engine  # noqa: F401 — registers every warning payload class
import slayer.mcp.server  # noqa: F401 — registers every warning payload class
from slayer.core.warnings import SlayerWarning
from slayer.database import engine_factory
from slayer.storage.base import StorageBackend
from tests._stored_upgrade_fixtures import (
    BACKENDS,
    DS,
    customers_v10,
    orders_v10,
    query_backed_v10,
    raw_store,
    run,
    seed_shop,
)

#: Last stored dataset version before stored spine relationships were dropped on load.
_PRE_SPINE_DROP = 24
#: Stored versions whose query extensions still carried columns.
_EXT_DATASET_V, _EXT_QUERY_V = 21, 13

OLD_WORDS = {"model", "column", "measure", "join", "formula", "models", "columns", "measures", "joins", "formulas"}

_SPINE_DOC: dict[str, Any] = {
    "version": _PRE_SPINE_DROP, "name": "ev", "sql_table": "orders", "data_source": DS,
    "columns": [
        {"name": "order_id", "type": "INT", "primary_key": True},
        {"name": "amount", "type": "DOUBLE"},
        {"name": "ordered_at", "type": "TIMESTAMP"},
        {"name": "ordered_day", "sql": "ordered_at", "type": "TIMESTAMP"},
    ],
    "joins": [{"target_model": "time_spine", "name": "spine_link", "join_pairs": [["ordered_at", "timestamp"]]}],
}
_SPINE_QUERY: dict[str, Any] = {
    "time_dimensions": [{"field": "time_spine.timestamp", "granularity": "month",
                         "date_range": ["2024-01-01", "2024-03-31"]}],
    "metrics": [{"metric": "sum(ev.amount)", "alias": "s"}],
}
_EXTENSION_DOC = query_backed_v10(name="rev_by_tier", version=_EXT_DATASET_V, stages=[{
    "version": _EXT_QUERY_V, "source_model": "orders",
    "extensions": [{"source_name": "customers", "columns": [{"name": "tier_label", "sql": "upper(region)"}]}],
    "dimensions": ["customers.tier_label"], "measures": [{"formula": "sum(amount)", "name": "rev"}],
}])


@pytest.fixture
def shop_ds(tmp_path: Path) -> Iterator[DatasourceConfig]:
    db = str(tmp_path / "shop.db")
    seed_shop(db, dialect="sqlite")
    ds = DatasourceConfig(name=DS, type="sqlite", database=db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


def payloads(caught: list[warnings.WarningMessage], *, kind: str) -> list[SlayerWarning]:
    """The structured payloads of kind ``kind`` the recorded warnings carry."""
    carried = [getattr(w.message, "payload", None) for w in caught]
    return [p for p in carried if isinstance(p, SlayerWarning) and p.kind == kind]


async def store(tmp_path: Path, *, backend: str, ds: DatasourceConfig, docs: list[dict]) -> StorageBackend:
    return await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds, models=docs)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_dropped_spine_relationship_is_named_in_the_vocabulary(
    tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
) -> None:
    storage = await store(tmp_path, backend=backend, ds=shop_ds, docs=[_SPINE_DOC])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        await run(storage, _SPINE_QUERY)
    [payload] = payloads(caught, kind="spine_relationship_dropped")
    dumped = payload.model_dump(mode="json")
    assert (dumped["dataset"], dumped["relationship"]) == ("ev", "spine_link")
    assert not {"model", "join"} & dumped.keys()
    assert payloads(caught, kind="spine_join_dropped") == []


@pytest.mark.parametrize("backend", BACKENDS)
async def test_an_inlined_extension_field_is_named_in_the_vocabulary(
    tmp_path: Path, shop_ds: DatasourceConfig, backend: str,
) -> None:
    storage = await store(tmp_path, backend=backend, ds=shop_ds, docs=[orders_v10(), customers_v10(), _EXTENSION_DOC])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        dataset = await storage.get_dataset("rev_by_tier", data_source=DS)
    assert dataset is not None
    [payload] = payloads(caught, kind="extension_field_inlined")
    dumped = payload.model_dump(mode="json")
    assert (dumped["document"], dumped["field"]) == ("rev_by_tier", "customers.tier_label")
    assert "column" not in dumped
    assert payloads(caught, kind="extension_column_inlined") == []


def _all_subclasses(cls: type) -> list[type]:
    out: list[type] = []
    for sub in cls.__subclasses__():
        out += [sub, *_all_subclasses(sub)]
    return out


def _old_word_in(name: str) -> bool:
    """``name`` holds an old word as a whole ``_``-separated part, outside the SQL ``semi_join`` carve-out."""
    return bool(OLD_WORDS & set(name.replace("semi_join", "semijoin").split("_")))


def test_no_warning_kind_uses_an_old_word() -> None:
    kinds = [
        cls.model_fields["kind"].default for cls in _all_subclasses(SlayerWarning)
        if "kind" in cls.model_fields and isinstance(cls.model_fields["kind"].default, str)
    ]
    assert kinds
    assert [k for k in kinds if _old_word_in(k)] == []


def test_no_warning_payload_field_uses_an_old_word() -> None:
    offending = [
        (cls.__name__, name) for cls in _all_subclasses(SlayerWarning) for name in cls.model_fields
        if _old_word_in(name)
    ]
    assert offending == []

