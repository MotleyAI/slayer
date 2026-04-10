"""Tests for YAML storage."""

import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> YAMLStorage:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


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


class TestModelStorage:
    def test_save_and_get(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        storage.save_model(sample_model)
        loaded = storage.get_model("test_model")
        assert loaded is not None
        assert loaded.name == "test_model"
        assert loaded.sql_table == "public.test_table"
        assert len(loaded.dimensions) == 2
        assert len(loaded.measures) == 1

    def test_list_models(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        assert storage.list_models() == []
        storage.save_model(sample_model)
        assert storage.list_models() == ["test_model"]

    def test_delete_model(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        storage.save_model(sample_model)
        assert storage.delete_model("test_model") is True
        assert storage.get_model("test_model") is None
        assert storage.delete_model("nonexistent") is False

    def test_get_nonexistent(self, storage: YAMLStorage) -> None:
        assert storage.get_model("nonexistent") is None

    def test_update_model(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        storage.save_model(sample_model)
        sample_model.description = "Updated description"
        storage.save_model(sample_model)
        loaded = storage.get_model("test_model")
        assert loaded.description == "Updated description"


class TestDatasourceStorage:
    def test_save_and_get(self, storage: YAMLStorage, sample_datasource: DatasourceConfig) -> None:
        storage.save_datasource(sample_datasource)
        loaded = storage.get_datasource("test_ds")
        assert loaded is not None
        assert loaded.name == "test_ds"
        assert loaded.type == "postgres"
        assert loaded.host == "localhost"

    def test_list_datasources(self, storage: YAMLStorage, sample_datasource: DatasourceConfig) -> None:
        assert storage.list_datasources() == []
        storage.save_datasource(sample_datasource)
        assert storage.list_datasources() == ["test_ds"]

    def test_delete_datasource(self, storage: YAMLStorage, sample_datasource: DatasourceConfig) -> None:
        storage.save_datasource(sample_datasource)
        assert storage.delete_datasource("test_ds") is True
        assert storage.get_datasource("test_ds") is None

    def test_env_var_resolution(self, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_DB_HOST", "resolved-host")
        ds = DatasourceConfig(name="env_ds", type="postgres", host="${TEST_DB_HOST}")
        storage.save_datasource(ds)
        loaded = storage.get_datasource("env_ds")
        assert loaded.host == "resolved-host"
