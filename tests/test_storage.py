"""Tests for YAML storage."""

import os
import stat
import tempfile

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage import yaml_storage as yaml_storage_module
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


def _fail_after_partial_write(data, stream=None, **kwargs):
    if stream is not None:
        stream.write("name: [")
    raise OSError("simulated disk full")


def test_atomic_write_preserves_permissions(tmp_path) -> None:
    existing = tmp_path / "existing.yaml"
    existing.write_text("old")
    existing.chmod(0o640)
    yaml_storage_module._atomic_write_text(path=str(existing), text="new")
    assert stat.S_IMODE(existing.stat().st_mode) == 0o640

    created = tmp_path / "created.yaml"
    old_umask = os.umask(0)
    try:
        yaml_storage_module._atomic_write_text(path=str(created), text="new")
    finally:
        os.umask(old_umask)
    assert stat.S_IMODE(created.stat().st_mode) == 0o600


def test_atomic_write_failure_preserves_target_and_cleans_temp(tmp_path, monkeypatch) -> None:
    path = tmp_path / "model.yaml"
    path.write_text("old")
    real_fdopen = os.fdopen

    class PartialWriter:
        def __init__(self, fd, *args, **kwargs):
            self.file = real_fdopen(fd, *args, **kwargs)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            self.file.close()

        def write(self, text):
            self.file.write(text[:1])
            self.file.flush()
            raise OSError("simulated temp-file write failure")

    monkeypatch.setattr(yaml_storage_module.os, "fdopen", PartialWriter)
    with pytest.raises(OSError, match="simulated temp-file write failure"):
        yaml_storage_module._atomic_write_text(path=str(path), text="new")

    assert path.read_text() == "old"
    assert list(tmp_path.iterdir()) == [path]


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

    async def test_failed_update_preserves_model(
        self,
        storage: YAMLStorage,
        sample_model: SlayerModel,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        await storage.save_model(sample_model)
        path = os.path.join(storage.models_dir, "test_ds", "test_model.yaml")
        original = open(path).read()  # NOSONAR(S7493) — sync fixture I/O is intentional

        monkeypatch.setattr("slayer.storage.yaml_storage.yaml.dump", _fail_after_partial_write)
        replacement = sample_model.model_copy(update={"description": "lost update"})
        with pytest.raises(OSError, match="simulated disk full"):
            await storage.save_model(replacement)

        assert open(path).read() == original  # NOSONAR(S7493) — sync fixture I/O is intentional
        assert (await storage.get_model("test_model")).description is None

    async def test_failed_sample_update_preserves_model(
        self,
        storage: YAMLStorage,
        sample_model: SlayerModel,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        await storage.save_model(sample_model)
        path = os.path.join(storage.models_dir, "test_ds", "test_model.yaml")
        original = open(path).read()  # NOSONAR(S7493) — sync fixture I/O is intentional

        monkeypatch.setattr("slayer.storage.yaml_storage.yaml.dump", _fail_after_partial_write)
        with pytest.raises(OSError, match="simulated disk full"):
            await storage.update_column_sampled(
                data_source="test_ds",
                model_name="test_model",
                column_name="name",
                sampled="2026-08-21T00:00:00Z",
                sampled_values=["alice"],
                distinct_count=1,
            )

        assert open(path).read() == original  # NOSONAR(S7493) — sync fixture I/O is intentional
        assert (await storage.get_model("test_model")).columns[1].sampled is None

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

    async def test_failed_update_preserves_datasource(
        self,
        storage: YAMLStorage,
        sample_datasource: DatasourceConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        await storage.save_datasource(sample_datasource)
        path = os.path.join(storage.datasources_dir, "test_ds.yaml")
        original = open(path).read()  # NOSONAR(S7493) — sync fixture I/O is intentional

        monkeypatch.setattr("slayer.storage.yaml_storage.yaml.dump", _fail_after_partial_write)
        replacement = sample_datasource.model_copy(update={"database": "lost-update"})
        with pytest.raises(OSError, match="simulated disk full"):
            await storage.save_datasource(replacement)

        assert open(path).read() == original  # NOSONAR(S7493) — sync fixture I/O is intentional
        assert (await storage.get_datasource("test_ds")).database == "testdb"

    async def test_failed_priority_update_preserves_priority(
        self,
        storage: YAMLStorage,
        sample_datasource: DatasourceConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        await storage.save_datasource(sample_datasource)
        await storage.set_datasource_priority(["test_ds"])
        original = open(storage._priority_path).read()  # NOSONAR(S7493) — sync fixture I/O is intentional

        monkeypatch.setattr("slayer.storage.yaml_storage.yaml.dump", _fail_after_partial_write)
        with pytest.raises(OSError, match="simulated disk full"):
            await storage.set_datasource_priority([])

        assert open(storage._priority_path).read() == original  # NOSONAR(S7493) — sync fixture I/O is intentional
        assert await storage.get_datasource_priority() == ["test_ds"]

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
