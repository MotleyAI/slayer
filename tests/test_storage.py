"""Tests for YAML storage."""

import os
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
    async def test_save_and_get(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        await storage.save_model(sample_model)
        loaded = await storage.get_model("test_model")
        assert loaded is not None
        assert loaded.name == "test_model"
        assert loaded.sql_table == "public.test_table"
        assert len(loaded.dimensions) == 2
        assert len(loaded.measures) == 1

    async def test_list_models(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        assert await storage.list_models() == []
        await storage.save_model(sample_model)
        assert await storage.list_models() == ["test_model"]

    async def test_delete_model(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        await storage.save_model(sample_model)
        assert await storage.delete_model("test_model") is True
        assert await storage.get_model("test_model") is None
        assert await storage.delete_model("nonexistent") is False

    async def test_get_nonexistent(self, storage: YAMLStorage) -> None:
        assert await storage.get_model("nonexistent") is None

    async def test_update_model(self, storage: YAMLStorage, sample_model: SlayerModel) -> None:
        await storage.save_model(sample_model)
        sample_model.description = "Updated description"
        await storage.save_model(sample_model)
        loaded = await storage.get_model("test_model")
        assert loaded.description == "Updated description"


class TestDatasourceStorage:
    async def test_save_and_get(self, storage: YAMLStorage, sample_datasource: DatasourceConfig) -> None:
        await storage.save_datasource(sample_datasource)
        loaded = await storage.get_datasource("test_ds")
        assert loaded is not None
        assert loaded.name == "test_ds"
        assert loaded.type == "postgres"
        assert loaded.host == "localhost"

    async def test_list_datasources(self, storage: YAMLStorage, sample_datasource: DatasourceConfig) -> None:
        assert await storage.list_datasources() == []
        await storage.save_datasource(sample_datasource)
        assert await storage.list_datasources() == ["test_ds"]

    async def test_delete_datasource(self, storage: YAMLStorage, sample_datasource: DatasourceConfig) -> None:
        await storage.save_datasource(sample_datasource)
        assert await storage.delete_datasource("test_ds") is True
        assert await storage.get_datasource("test_ds") is None

    async def test_env_var_resolution(self, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_DB_HOST", "resolved-host")
        ds = DatasourceConfig(name="env_ds", type="postgres", host="${TEST_DB_HOST}")
        await storage.save_datasource(ds)
        loaded = await storage.get_datasource("env_ds")
        assert loaded.host == "resolved-host"

    async def test_malformed_yaml_raises_valueerror(self, storage: YAMLStorage) -> None:
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        with pytest.raises(ValueError, match="Datasource 'bad': invalid YAML"):
            await storage.get_datasource("bad")

    async def test_invalid_config_raises_valueerror(self, storage: YAMLStorage) -> None:
        path = os.path.join(storage.datasources_dir, "bad_type.yaml")
        with open(path, "w") as f:
            f.write("name: bad_type\nport: not_a_number\n")
        with pytest.raises(ValueError, match="Datasource 'bad_type': invalid config"):
            await storage.get_datasource("bad_type")

    async def test_unresolved_env_var_raises_valueerror(self, storage: YAMLStorage) -> None:
        ds = DatasourceConfig(
            name="missing_env", type="postgres", host="${NONEXISTENT_VAR_12345}"
        )
        await storage.save_datasource(ds)
        with pytest.raises(ValueError, match="unresolved environment variable"):
            await storage.get_datasource("missing_env")

    async def test_malformed_datasource_does_not_break_list(self, storage: YAMLStorage) -> None:
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        names = await storage.list_datasources()
        assert "bad" in names
