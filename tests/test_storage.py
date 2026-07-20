"""Tests for YAML storage."""

import os
import tempfile

import pytest
import yaml

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
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
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
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
        assert len(loaded.columns) == 3
        assert loaded.measures == []

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

    async def test_case_sensitive_model_ids_do_not_alias(
        self, storage: YAMLStorage, sample_model: SlayerModel
    ) -> None:
        upper = sample_model.model_copy(
            update={"name": "X", "sql_table": "public.upper"},
        )
        lower = sample_model.model_copy(
            update={"name": "x", "sql_table": "public.lower"},
        )
        legacy_dir = os.path.join(storage.models_dir, "test_ds")
        os.makedirs(legacy_dir)
        with open(os.path.join(legacy_dir, "X.yaml"), "w") as f:  # NOSONAR(S7493) — test writes a tiny local fixture; sync I/O is intentional
            yaml.safe_dump(
                upper.model_dump(mode="json", exclude_none=True), f,
            )
        storage = YAMLStorage(base_dir=storage.base_dir)
        assert not os.path.exists(os.path.join(legacy_dir, "X.yaml"))
        await storage.save_model(lower)

        assert (
            await storage.get_model("X", data_source="test_ds")
        ).sql_table == "public.upper"
        assert (
            await storage.get_model("x", data_source="test_ds")
        ).sql_table == "public.lower"

    async def test_empty_model_file_raises_clear_error(
        self, storage: YAMLStorage, sample_model: SlayerModel
    ) -> None:
        # A zero-byte model file (disk-full / interrupted write) must surface
        # an actionable error, not a bare Pydantic model_type failure.
        await storage.save_model(sample_model)
        path = os.path.join(storage.models_dir, "test_ds", "test_model.yaml")
        open(path, "w").close()  # NOSONAR(S7493) — test corrupts a tiny local fixture file; sync I/O is intentional

        with pytest.raises(ValueError, match="empty or corrupt") as excinfo:
            await storage.get_model("test_model")
        assert "test_model" in str(excinfo.value)
        assert "slayer ingest" in str(excinfo.value)

    async def test_truncated_model_file_raises_clear_error(
        self, storage: YAMLStorage, sample_model: SlayerModel
    ) -> None:
        # A file cut off mid-write (full disk) is invalid YAML; the error must
        # name the file and the remediation, not surface a bare yaml trace.
        await storage.save_model(sample_model)
        path = os.path.join(storage.models_dir, "test_ds", "test_model.yaml")
        with open(path, "w") as f:  # NOSONAR(S7493) — test corrupts a tiny local fixture file; sync I/O is intentional
            f.write('name: "test_model\ncolumns:\n  - name: "id\n')

        with pytest.raises(ValueError, match="invalid YAML") as excinfo:
            await storage.get_model("test_model")
        assert "test_model.yaml" in str(excinfo.value)
        assert "slayer ingest" in str(excinfo.value)


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

    async def test_case_sensitive_datasource_ids_do_not_alias(
        self, storage: YAMLStorage, sample_datasource: DatasourceConfig
    ) -> None:
        upper = sample_datasource.model_copy(
            update={"name": "X", "host": "upper"},
        )
        lower = sample_datasource.model_copy(
            update={"name": "x", "host": "lower"},
        )
        with open(  # NOSONAR(S7493) — test writes a tiny local fixture; sync I/O is intentional
            os.path.join(storage.datasources_dir, "X.yaml"), "w",
        ) as f:
            yaml.safe_dump(
                upper.model_dump(mode="json", exclude_none=True), f,
            )
        storage = YAMLStorage(base_dir=storage.base_dir)
        assert not os.path.exists(
            os.path.join(storage.datasources_dir, "X.yaml"),
        )
        await storage.save_datasource(lower)

        assert (await storage.get_datasource("X")).host == "upper"
        assert (await storage.get_datasource("x")).host == "lower"

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

    def test_migration_refuses_divergent_duplicate(
        self, tmp_path,
    ) -> None:
        datasources_dir = tmp_path / "datasources"
        encoded_dir = datasources_dir / ".encoded"
        encoded_dir.mkdir(parents=True)
        (tmp_path / "models").mkdir()
        (tmp_path / "memories").mkdir()
        (datasources_dir / "x.yaml").write_text(
            "name: x\ntype: postgres\nhost: raw\n",
        )
        (encoded_dir / "78.yaml").write_text(
            "name: x\ntype: postgres\nhost: encoded\n",
        )

        with pytest.raises(ValueError, match="different content"):
            YAMLStorage(base_dir=str(tmp_path))

        assert (datasources_dir / "x.yaml").exists()
        assert (encoded_dir / "78.yaml").exists()
