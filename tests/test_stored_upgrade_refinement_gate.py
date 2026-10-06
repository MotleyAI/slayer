"""Live type refinement on load applies only below model v8 (spec: models/stored-upgrade)."""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory
from slayer.storage import base as storage_base
from slayer.storage import migrations as mig
from slayer.storage.sqlite_conn import transaction
from tests._stored_upgrade_fixtures import BACKENDS, DS, column_of, raw_doc, raw_store


def _items(*, version: int, amount_type: str = "DOUBLE") -> dict:
    return {
        "version": version, "name": "items", "sql_table": "items", "data_source": DS,
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "amount", "type": amount_type},
        ],
    }


@pytest.fixture
def no_live_access(monkeypatch) -> list[str]:
    """Record (and refuse) every live refinement or engine creation."""
    calls: list[str] = []

    def _refine(*_a, **_k):
        calls.append("refine")
        raise AssertionError("live refinement attempted")

    def _engine(*_a, **_k):
        calls.append("get_engine")
        raise AssertionError("engine creation attempted")

    monkeypatch.setattr(storage_base, "refine_dict_with_live_schema", _refine)
    monkeypatch.setattr(engine_factory, "get_engine", _engine)
    return calls


@pytest.mark.parametrize("backend", BACKENDS)
async def test_v10_model_loads_without_datasource_entry(tmp_path, backend):
    storage = await raw_store(backend=backend, base=str(tmp_path), datasource=None, models=[_items(version=10)])
    model = await storage.get_model("items", data_source=DS)
    assert model is not None
    assert column_of(model, "amount").type is DataType.DOUBLE
    assert (await raw_doc(storage, "items"))["version"] == mig.CURRENT_VERSIONS["SlayerModel"]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_v10_model_loads_with_unreachable_datasource(tmp_path, backend, no_live_access):
    ds = DatasourceConfig(name=DS, type="postgres", host="127.0.0.1", port=1,
                          database="shop", username="u", password="p")
    storage = await raw_store(backend=backend, base=str(tmp_path), datasource=ds, models=[_items(version=10)])
    model = await storage.get_model("items", data_source=DS)
    assert model is not None
    assert column_of(model, "amount").type is DataType.DOUBLE
    assert no_live_access == []


@pytest.mark.parametrize("backend", BACKENDS)
async def test_pre_v8_model_still_requires_its_datasource(tmp_path, backend):
    storage = await raw_store(backend=backend, base=str(tmp_path), datasource=None, models=[_items(version=5)])
    with pytest.raises(ValueError, match="unavailable for type refinement"):
        await storage.get_model("items", data_source=DS)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_v7_model_is_still_refined(tmp_path, backend):
    db = str(tmp_path / "live.db")
    with transaction(db) as cur:
        cur.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER)")
        cur.executemany("INSERT INTO items VALUES (?, ?)", [(1, 10), (2, 20)])
    ds = DatasourceConfig(name=DS, type="sqlite", database=db)
    storage = await raw_store(backend=backend, base=str(tmp_path / "store"), datasource=ds,
                              models=[_items(version=7)])
    try:
        model = await storage.get_model("items", data_source=DS)
    finally:
        engine_factory.invalidate_engine(ds)
    assert model is not None
    assert column_of(model, "amount").type is DataType.INT
