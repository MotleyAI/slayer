"""Per-column sample-value persistence (DEV-1375).

Pins ``StorageBackend.update_column_sampled`` semantics across the ABC
contract, the YAML and SQLite implementations, and the
``JoinSyncStorage`` delegating wrapper.
"""

from __future__ import annotations

import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage.base import resolve_storage
from slayer.storage.join_sync import JoinSyncStorage
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


def _make_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="ds",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE,
                   description="Total amount."),
            Column(name="status", type=DataType.TEXT),
        ],
    )


# ---------------------------------------------------------------------------
# YAML
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_yaml_update_column_sampled_persists() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_model(_make_model())
        await storage.update_column_sampled(
            data_source="ds", model_name="orders",
            column_name="amount", sampled="0.0 .. 9999.99",
        )
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert loaded.get_column("amount").sampled == "0.0 .. 9999.99"
        # Other columns untouched.
        assert loaded.get_column("id").sampled is None
        assert loaded.get_column("status").sampled is None


@pytest.mark.asyncio
async def test_yaml_update_column_sampled_to_none_clears() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        m = _make_model()
        m.columns[1].sampled = "stale value"
        await storage.save_model(m)
        await storage.update_column_sampled(
            data_source="ds", model_name="orders",
            column_name="amount", sampled=None,
        )
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded.get_column("amount").sampled is None


@pytest.mark.asyncio
async def test_yaml_update_column_sampled_preserves_other_fields() -> None:
    """Read-modify-write must not lose adjacent fields."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        m = _make_model()
        m.columns[1].description = "preserve me"
        m.columns[1].label = "Order Amount"
        await storage.save_model(m)
        await storage.update_column_sampled(
            data_source="ds", model_name="orders",
            column_name="amount", sampled="0 .. 100",
        )
        loaded = await storage.get_model("orders", data_source="ds")
        col = loaded.get_column("amount")
        assert col.description == "preserve me"
        assert col.label == "Order Amount"
        assert col.sampled == "0 .. 100"


@pytest.mark.asyncio
async def test_yaml_update_column_sampled_unknown_model_errors() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        with pytest.raises((KeyError, ValueError, FileNotFoundError)):
            await storage.update_column_sampled(
                data_source="ds", model_name="nope",
                column_name="amount", sampled="x",
            )


@pytest.mark.asyncio
async def test_yaml_update_column_sampled_unknown_column_errors() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_model(_make_model())
        with pytest.raises((KeyError, ValueError)):
            await storage.update_column_sampled(
                data_source="ds", model_name="orders",
                column_name="nope", sampled="x",
            )


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sqlite_update_column_sampled_persists() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        storage = SQLiteStorage(db_path=db_path)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_model(_make_model())
        await storage.update_column_sampled(
            data_source="ds", model_name="orders",
            column_name="amount", sampled="0.0 .. 9999.99",
        )
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded.get_column("amount").sampled == "0.0 .. 9999.99"


@pytest.mark.asyncio
async def test_sqlite_update_column_sampled_preserves_other_fields() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        storage = SQLiteStorage(db_path=db_path)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        m = _make_model()
        m.columns[1].description = "preserve me"
        await storage.save_model(m)
        await storage.update_column_sampled(
            data_source="ds", model_name="orders",
            column_name="amount", sampled="0 .. 100",
        )
        loaded = await storage.get_model("orders", data_source="ds")
        col = loaded.get_column("amount")
        assert col.description == "preserve me"
        assert col.sampled == "0 .. 100"


# ---------------------------------------------------------------------------
# JoinSync delegation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_join_sync_delegates_update_column_sampled() -> None:
    """The wrapper is what the factory always returns — must pass-through."""
    with tempfile.TemporaryDirectory() as tmpdir:
        wrapped = resolve_storage(tmpdir)  # returns JoinSyncStorage
        assert isinstance(wrapped, JoinSyncStorage)
        await wrapped.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await wrapped.save_model(_make_model())
        await wrapped.update_column_sampled(
            data_source="ds", model_name="orders",
            column_name="amount", sampled="0 .. 100",
        )
        loaded = await wrapped.get_model("orders", data_source="ds")
        assert loaded.get_column("amount").sampled == "0 .. 100"


@pytest.mark.asyncio
async def test_join_sync_delegates_to_sqlite_inner() -> None:
    """Same delegation, SQLite-backed."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        wrapped = resolve_storage(db_path)  # returns JoinSyncStorage(SQLiteStorage)
        assert isinstance(wrapped, JoinSyncStorage)
        await wrapped.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await wrapped.save_model(_make_model())
        await wrapped.update_column_sampled(
            data_source="ds", model_name="orders",
            column_name="amount", sampled="0 .. 100",
        )
        loaded = await wrapped.get_model("orders", data_source="ds")
        assert loaded.get_column("amount").sampled == "0 .. 100"


# ---------------------------------------------------------------------------
# Two-update independence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_sampled_updates_to_different_columns_dont_clobber_each_other() -> None:
    """Sequential updates to different columns must accumulate, not overwrite."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))
        await storage.save_model(_make_model())
        await storage.update_column_sampled(
            data_source="ds", model_name="orders",
            column_name="amount", sampled="0 .. 100",
        )
        await storage.update_column_sampled(
            data_source="ds", model_name="orders",
            column_name="status", sampled="paid, refunded",
        )
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded.get_column("amount").sampled == "0 .. 100"
        assert loaded.get_column("status").sampled == "paid, refunded"
