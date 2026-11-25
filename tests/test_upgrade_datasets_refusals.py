"""``upgrade_datasets`` refuses each failing document on its own, as ``slayer storage upgrade`` does.

Spec: models/stored-upgrade › Documents SLayer does not own upgrade without a store.
"""

from __future__ import annotations

import asyncio
import copy
from pathlib import Path

from slayer.storage.migrations import CURRENT_VERSIONS
from slayer.storage.upgrade import StoreUpgradeReport, upgrade_datasets, upgrade_store
from tests import _stored_upgrade_fixtures as su
from tests._integrator_1x import DATA_SOURCE, raw_documents
from tests._storage_upgrade_cli import warehouse_store

PRE_V8 = {"version": 5, "name": "items", "sql_table": "items", "data_source": su.DS,
          "columns": [{"name": "qty", "sql": "qty", "type": "number"}]}


def _bad_datatype() -> dict:
    bad = copy.deepcopy(raw_documents()["customers"])
    bad["name"] = "bad"
    bad["columns"][0]["type"] = "WIBBLE"
    return bad


def _store_detail(report: StoreUpgradeReport, shown: str) -> str:
    [outcome] = [o for o in report.outcomes if o.name == shown]
    assert outcome.status == "not upgraded", outcome
    assert outcome.detail is not None
    return outcome.detail


def test_an_invalid_document_is_refused_on_its_own() -> None:
    result = upgrade_datasets(documents=[raw_documents()["customers"], _bad_datatype()])
    assert [d["name"] for d in result.documents] == ["customers"]
    assert result.documents[0]["version"] == CURRENT_VERSIONS["SlayerModel"]
    [refusal] = result.refused
    assert refusal.name == "bad"
    assert "WIBBLE" in refusal.reason


def test_an_invalid_document_first_still_returns_the_rest() -> None:
    result = upgrade_datasets(documents=[_bad_datatype(), raw_documents()["customers"]])
    assert [d["name"] for d in result.documents] == ["customers"]
    assert [r.name for r in result.refused] == ["bad"]


def test_the_same_invalid_reason_as_the_store_upgrade(tmp_path: Path) -> None:
    storage = warehouse_store(backend="yaml", base=str(tmp_path / "store"),
                              datasets=[raw_documents()["customers"], _bad_datatype()])
    report = asyncio.run(upgrade_store(storage))
    [refusal] = upgrade_datasets(documents=[raw_documents()["customers"], _bad_datatype()]).refused
    assert refusal.reason in _store_detail(report, f"{DATA_SOURCE}.bad")


def test_the_same_live_refinement_reason_as_the_store_upgrade(tmp_path: Path) -> None:
    storage = asyncio.run(su.raw_store(backend="yaml", base=str(tmp_path / "store"), datasource=None,
                                       models=[PRE_V8]))
    report = asyncio.run(upgrade_store(storage))
    [refusal] = upgrade_datasets(documents=[PRE_V8], datasource_type="duckdb").refused
    assert refusal.reason == _store_detail(report, f"{su.DS}.items")


def test_the_same_dialect_reason_as_the_store_upgrade(tmp_path: Path) -> None:
    storage = warehouse_store(backend="yaml", base=str(tmp_path / "store"), datasource=False)
    report = asyncio.run(upgrade_store(storage))
    [refusal] = upgrade_datasets(documents=list(raw_documents().values())).refused
    common = f"its field sql waits for datasource {DATA_SOURCE!r}'s dialect"
    assert refusal.reason.startswith(common)
    assert _store_detail(report, f"{DATA_SOURCE}.orders").startswith(common)
