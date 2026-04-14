"""Tests for SQLite storage."""

import tempfile
import os

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.async_utils import run_sync


@pytest.fixture
def storage() -> SQLiteStorage:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        yield SQLiteStorage(db_path=db_path)


@pytest.fixture
def sample_model() -> SlayerModel:
    return SlayerModel(
        name="test_model",
        sql_table="public.test_table",
        data_source="test_ds",
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="name", sql="name", type=DataType.STRING),
        ],
        measures=[
            Measure(name="revenue", sql="amount"),
        ],
    )


@pytest.fixture
def sample_datasource() -> DatasourceConfig:
    return DatasourceConfig(
        name="test_ds",
        type="postgres",
        host="localhost",
        port=5432,
        database="testdb",
        username="user",
        password="pass",
    )


class TestSQLiteModelStorage:
    def test_save_and_get(self, storage: SQLiteStorage, sample_model: SlayerModel) -> None:
        run_sync(storage.save_model(sample_model))
        loaded = run_sync(storage.get_model("test_model"))
        assert loaded is not None
        assert loaded.name == "test_model"
        assert loaded.sql_table == "public.test_table"
        assert len(loaded.dimensions) == 2

    def test_list_models(self, storage: SQLiteStorage, sample_model: SlayerModel) -> None:
        assert run_sync(storage.list_models()) == []
        run_sync(storage.save_model(sample_model))
        assert run_sync(storage.list_models()) == ["test_model"]

    def test_delete_model(self, storage: SQLiteStorage, sample_model: SlayerModel) -> None:
        run_sync(storage.save_model(sample_model))
        assert run_sync(storage.delete_model("test_model")) is True
        assert run_sync(storage.get_model("test_model")) is None
        assert run_sync(storage.delete_model("nonexistent")) is False

    def test_update_model(self, storage: SQLiteStorage, sample_model: SlayerModel) -> None:
        run_sync(storage.save_model(sample_model))
        sample_model.description = "Updated"
        run_sync(storage.save_model(sample_model))
        loaded = run_sync(storage.get_model("test_model"))
        assert loaded.description == "Updated"


class TestSQLiteDatasourceStorage:
    def test_save_and_get(self, storage: SQLiteStorage, sample_datasource: DatasourceConfig) -> None:
        run_sync(storage.save_datasource(sample_datasource))
        loaded = run_sync(storage.get_datasource("test_ds"))
        assert loaded is not None
        assert loaded.type == "postgres"

    def test_list_datasources(self, storage: SQLiteStorage, sample_datasource: DatasourceConfig) -> None:
        assert run_sync(storage.list_datasources()) == []
        run_sync(storage.save_datasource(sample_datasource))
        assert run_sync(storage.list_datasources()) == ["test_ds"]

    def test_delete_datasource(self, storage: SQLiteStorage, sample_datasource: DatasourceConfig) -> None:
        run_sync(storage.save_datasource(sample_datasource))
        assert run_sync(storage.delete_datasource("test_ds")) is True
        assert run_sync(storage.get_datasource("test_ds")) is None

    def test_env_var_resolution(self, storage: SQLiteStorage, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SQLITE_TEST_HOST", "resolved-host")
        ds = DatasourceConfig(name="env_ds", type="postgres", host="${SQLITE_TEST_HOST}")
        run_sync(storage.save_datasource(ds))
        loaded = run_sync(storage.get_datasource("env_ds"))
        assert loaded.host == "resolved-host"
