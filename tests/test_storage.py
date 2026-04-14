"""Tests for YAML storage."""

import os
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.storage.yaml_storage import YAMLStorage
from slayer.async_utils import run_sync


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
        run_sync(storage.save_model(sample_model))
        loaded = run_sync(storage.get_model("test_model"))
        assert loaded is not None
        assert loaded.name == "test_model"
        assert loaded.sql_table == "public.test_table"
        assert len(loaded.dimensions) == 2
        assert len(loaded.measures) == 1

    def test_list_models(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        assert run_sync(storage.list_models()) == []
        run_sync(storage.save_model(sample_model))
        assert run_sync(storage.list_models()) == ["test_model"]

    def test_delete_model(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        run_sync(storage.save_model(sample_model))
        assert run_sync(storage.delete_model("test_model")) is True
        assert run_sync(storage.get_model("test_model")) is None
        assert run_sync(storage.delete_model("nonexistent")) is False

    def test_get_nonexistent(self, storage: YAMLStorage) -> None:
        assert run_sync(storage.get_model("nonexistent")) is None

    def test_update_model(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        run_sync(storage.save_model(sample_model))
        sample_model.description = "Updated description"
        run_sync(storage.save_model(sample_model))
        loaded = run_sync(storage.get_model("test_model"))
        assert loaded.description == "Updated description"


class TestDatasourceStorage:
    def test_save_and_get(self, storage: YAMLStorage, sample_datasource: DatasourceConfig) -> None:
        run_sync(storage.save_datasource(sample_datasource))
        loaded = run_sync(storage.get_datasource("test_ds"))
        assert loaded is not None
        assert loaded.name == "test_ds"
        assert loaded.type == "postgres"
        assert loaded.host == "localhost"

    def test_list_datasources(self, storage: YAMLStorage, sample_datasource: DatasourceConfig) -> None:
        assert run_sync(storage.list_datasources()) == []
        run_sync(storage.save_datasource(sample_datasource))
        assert run_sync(storage.list_datasources()) == ["test_ds"]

    def test_delete_datasource(self, storage: YAMLStorage, sample_datasource: DatasourceConfig) -> None:
        run_sync(storage.save_datasource(sample_datasource))
        assert run_sync(storage.delete_datasource("test_ds")) is True
        assert run_sync(storage.get_datasource("test_ds")) is None

    def test_env_var_resolution(self, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_DB_HOST", "resolved-host")
        ds = DatasourceConfig(name="env_ds", type="postgres", host="${TEST_DB_HOST}")
        run_sync(storage.save_datasource(ds))
        loaded = run_sync(storage.get_datasource("env_ds"))
        assert loaded.host == "resolved-host"

    def test_malformed_yaml_raises_valueerror(self, storage: YAMLStorage) -> None:
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        with pytest.raises(ValueError, match="Datasource 'bad': invalid YAML"):
            run_sync(storage.get_datasource("bad"))

    def test_invalid_config_raises_valueerror(self, storage: YAMLStorage) -> None:
        path = os.path.join(storage.datasources_dir, "bad_type.yaml")
        with open(path, "w") as f:
            f.write("name: bad_type\nport: not_a_number\n")
        with pytest.raises(ValueError, match="Datasource 'bad_type': invalid config"):
            run_sync(storage.get_datasource("bad_type"))

    def test_unresolved_env_var_raises_valueerror(self, storage: YAMLStorage) -> None:
        ds = DatasourceConfig(
            name="missing_env", type="postgres", host="${NONEXISTENT_VAR_12345}"
        )
        run_sync(storage.save_datasource(ds))
        with pytest.raises(ValueError, match="unresolved environment variable"):
            run_sync(storage.get_datasource("missing_env"))

    def test_malformed_datasource_does_not_break_list(self, storage: YAMLStorage) -> None:
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        names = run_sync(storage.list_datasources())
        assert "bad" in names
