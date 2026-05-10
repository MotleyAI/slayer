"""v5 → v6 schema migration for SlayerModel (DEV-1375).

v6 adds a single new optional field: ``Column.sampled: Optional[str] = None``.
The migration itself is a no-op forward — existing payloads load with
``sampled=None`` everywhere; first subsequent ingest / refresh-samples
populates the cache.
"""

from __future__ import annotations

import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.storage import migrations as mig
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


def test_current_slayer_model_version_is_v6() -> None:
    assert mig.CURRENT_VERSIONS["SlayerModel"] == 6


def test_slayer_model_default_version_is_v6() -> None:
    m = SlayerModel(name="orders", sql_table="orders", data_source="ds")
    assert m.version == 6


def test_slayer_model_dump_writes_v6() -> None:
    m = SlayerModel(name="orders", sql_table="orders", data_source="ds")
    assert m.model_dump(mode="json", exclude_none=True)["version"] == 6


def test_v5_to_v6_no_op_forward() -> None:
    out = mig.migrate("SlayerModel", {
        "version": 5,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [{"name": "id", "type": "INT"}],
    })
    assert out["version"] == 6
    # The new field is not introduced by the migrator (Pydantic default fills
    # it in on validation). The dict shape is otherwise unchanged.
    assert out["columns"] == [{"name": "id", "type": "INT"}]


def test_v5_payload_loads_with_sampled_none() -> None:
    raw = {
        "version": 5,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "amount", "type": "DOUBLE"},
        ],
    }
    m = SlayerModel.model_validate(raw)
    assert m.version == 6
    for col in m.columns:
        assert col.sampled is None


def test_v6_payload_round_trips_with_sampled_value() -> None:
    raw = {
        "version": 6,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [
            {"name": "amount", "type": "DOUBLE", "sampled": "0.0 .. 9999.99"},
        ],
    }
    m = SlayerModel.model_validate(raw)
    assert m.version == 6
    assert m.columns[0].sampled == "0.0 .. 9999.99"
    dumped = m.model_dump(mode="json", exclude_none=True)
    assert dumped["columns"][0]["sampled"] == "0.0 .. 9999.99"


# ---------------------------------------------------------------------------
# Round-trip via storage backends
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_yaml_round_trips_v5_payload_to_v6_with_sampled_none() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        from slayer.core.models import DatasourceConfig
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        # Save a v5-shaped model directly via Pydantic; loading will migrate.
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[Column(name="amount", type=DataType.DOUBLE)],
        )
        await storage.save_model(model)
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert loaded.version == 6
        assert loaded.columns[0].sampled is None


@pytest.mark.asyncio
async def test_sqlite_round_trips_v5_payload_to_v6_with_sampled_none() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        storage = SQLiteStorage(db_path=db_path)
        from slayer.core.models import DatasourceConfig
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        model = SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[Column(name="amount", type=DataType.DOUBLE)],
        )
        await storage.save_model(model)
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert loaded.version == 6
        assert loaded.columns[0].sampled is None


# ---------------------------------------------------------------------------
# Backward compat: v4 → v5 → v6 chain still works
# ---------------------------------------------------------------------------


def test_v4_payload_walks_through_chain_to_v6() -> None:
    raw = {
        "version": 4,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [{"name": "amount", "type": "number"}],  # legacy lowercase v4
    }
    out = mig.migrate("SlayerModel", raw)
    assert out["version"] == 6
    # v4→v5 normalised the legacy lowercase to canonical "DOUBLE"
    assert out["columns"][0]["type"] == "DOUBLE"
