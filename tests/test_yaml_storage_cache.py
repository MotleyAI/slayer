"""mtime-keyed model/datasource load cache for YAMLStorage (DEV-1816)."""

import os
import tempfile

import pytest
import yaml as _yaml

from slayer.core import models as _core_models
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage import migrations as _mig
from slayer.storage import yaml_storage as _yaml_storage
from slayer.storage.base import resolve_storage
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> YAMLStorage:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


def _model(*, name: str = "m", ds: str = "ds", sql_table: str = "public.t") -> SlayerModel:
    return SlayerModel(
        name=name,
        sql_table=sql_table,
        data_source=ds,
        columns=[Column(name="id", sql="id", type=DataType.TEXT, primary_key=True)],
    )


def _install_counter(monkeypatch, obj, attr: str) -> dict:
    calls = {"n": 0}
    orig = getattr(obj, attr)

    def wrapper(*args, **kwargs):
        calls["n"] += 1
        return orig(*args, **kwargs)

    monkeypatch.setattr(obj, attr, wrapper)
    return calls


def _bump_mtime(path: str) -> None:
    st = os.stat(path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 5_000_000))


# ---- models: cache hits skip work -----------------------------------------


async def test_hit_skips_parse(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_model(_model())
    loads = _install_counter(monkeypatch, _yaml_storage.yaml, "safe_load")
    assert await storage.get_model("m", data_source="ds") is not None
    assert await storage.get_model("m", data_source="ds") is not None
    assert loads["n"] == 1  # second get was a cache hit


async def test_hit_skips_validate_and_migrate(storage: YAMLStorage, monkeypatch) -> None:
    # A parse runs SlayerModel.model_validate, whose before-validator invokes the
    # migrations pass (_migrate_schema). A cache hit skips both.
    await storage.save_model(_model())
    validates = _install_counter(monkeypatch, SlayerModel, "model_validate")
    migrates = _install_counter(monkeypatch, _core_models, "_migrate_schema")
    await storage.get_model("m", data_source="ds")  # miss
    v0, m0 = validates["n"], migrates["n"]
    assert v0 >= 1
    assert m0 >= 1
    await storage.get_model("m", data_source="ds")  # hit
    assert validates["n"] == v0
    assert migrates["n"] == m0


# ---- models: invalidation --------------------------------------------------


async def test_save_model_evicts(storage: YAMLStorage) -> None:
    await storage.save_model(_model(sql_table="public.a"))
    path = storage._model_path("ds", "m")
    assert (await storage.get_model("m", data_source="ds")).sql_table == "public.a"
    assert path in storage._model_cache
    await storage.save_model(_model(sql_table="public.b"))
    assert path not in storage._model_cache
    assert (await storage.get_model("m", data_source="ds")).sql_table == "public.b"


async def test_external_edit_invalidates(storage: YAMLStorage) -> None:
    await storage.save_model(_model(sql_table="public.a"))
    await storage.get_model("m", data_source="ds")  # populate
    path = storage._model_path("ds", "m")
    size_before = os.stat(path).st_size
    with open(path, "w") as f:
        _yaml.dump(_model(sql_table="public.b").model_dump(mode="json", exclude_none=True), f)
    _bump_mtime(path)
    # Same byte length (only one char differs) — invalidation is driven by mtime, not size.
    assert os.stat(path).st_size == size_before
    assert (await storage.get_model("m", data_source="ds")).sql_table == "public.b"


async def test_same_mtime_size_rewrite_invalidated_by_ctime(storage: YAMLStorage) -> None:
    await storage.save_model(_model(sql_table="public.a"))
    await storage.get_model("m", data_source="ds")  # populate
    path = storage._model_path("ds", "m")
    st = os.stat(path)
    with open(path, "w") as f:
        _yaml.dump(_model(sql_table="public.b").model_dump(mode="json", exclude_none=True), f)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns))  # restore the original mtime
    after = os.stat(path)
    assert after.st_mtime_ns == st.st_mtime_ns  # mtime preserved
    assert after.st_size == st.st_size  # same size
    # ctime advanced (write + utime bumped it), so the key differs -> new content.
    assert (await storage.get_model("m", data_source="ds")).sql_table == "public.b"


async def test_external_delete_evicts_model_cache(storage: YAMLStorage) -> None:
    await storage.save_model(_model())
    await storage.get_model("m", data_source="ds")  # populate
    path = storage._model_path("ds", "m")
    assert path in storage._model_cache
    os.remove(path)  # external delete
    assert await storage.get_model("m", data_source="ds") is None
    assert path not in storage._model_cache


async def test_external_delete_evicts_datasource_cache(storage: YAMLStorage) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    assert path in storage._datasource_cache
    os.remove(path)  # external delete
    assert await storage.get_datasource("pg") is None
    assert path not in storage._datasource_cache


def _stat_key_raises_once(monkeypatch):
    """Make the next _stat_key call raise FileNotFoundError (file deleted between
    the existence check and the stat), then behave normally."""
    orig = _yaml_storage._stat_key
    state = {"fired": False}

    def flaky(path):
        if not state["fired"]:
            state["fired"] = True
            raise FileNotFoundError(path)
        return orig(path)

    monkeypatch.setattr(_yaml_storage, "_stat_key", flaky)


async def test_model_delete_race_at_stat_returns_none_and_evicts(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_model(_model())
    await storage.get_model("m", data_source="ds")  # populate
    path = storage._model_path("ds", "m")
    assert path in storage._model_cache
    _stat_key_raises_once(monkeypatch)
    assert await storage.get_model("m", data_source="ds") is None
    assert path not in storage._model_cache


async def test_datasource_delete_race_at_stat_returns_none_and_evicts(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    assert path in storage._datasource_cache
    _stat_key_raises_once(monkeypatch)
    assert await storage.get_datasource("pg") is None
    assert path not in storage._datasource_cache


async def test_update_column_sampled_evicts(storage: YAMLStorage) -> None:
    await storage.save_model(_model())
    await storage.get_model("m", data_source="ds")  # populate
    await storage.update_column_sampled(
        data_source="ds",
        model_name="m",
        column_name="id",
        sampled="s-val",
        sampled_values=["a"],
        distinct_count=1,
    )
    reloaded = await storage.get_model("m", data_source="ds")
    assert reloaded.columns[0].sampled == "s-val"


async def test_delete_model_evicts(storage: YAMLStorage) -> None:
    await storage.save_model(_model())
    await storage.get_model("m", data_source="ds")  # populate
    path = storage._model_path("ds", "m")
    assert path in storage._model_cache
    await storage.delete_model("m", data_source="ds")
    assert path not in storage._model_cache
    assert await storage.get_model("m", data_source="ds") is None


async def test_failed_save_evicts_before_write(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_model(_model(sql_table="public.a"))
    await storage.get_model("m", data_source="ds")  # populate
    path = storage._model_path("ds", "m")
    assert path in storage._model_cache

    def boom(*args, **kwargs):
        raise RuntimeError("write failed")

    replacement = _model(sql_table="public.b")
    monkeypatch.setattr(_yaml_storage.yaml, "dump", boom)
    with pytest.raises(RuntimeError):
        await storage.save_model(replacement)
    assert path not in storage._model_cache  # evicted before the failed write


def _raise(*args, **kwargs):
    raise RuntimeError("io failed")


async def test_failed_update_column_sampled_evicts(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_model(_model())
    await storage.get_model("m", data_source="ds")  # populate
    path = storage._model_path("ds", "m")
    assert path in storage._model_cache
    monkeypatch.setattr(_yaml_storage.yaml, "dump", _raise)
    with pytest.raises(RuntimeError):
        await storage.update_column_sampled(
            data_source="ds", model_name="m", column_name="id",
            sampled="x", sampled_values=None, distinct_count=None,
        )
    assert path not in storage._model_cache


async def test_failed_delete_model_evicts(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_model(_model())
    await storage.get_model("m", data_source="ds")  # populate
    path = storage._model_path("ds", "m")
    assert path in storage._model_cache
    monkeypatch.setattr(_yaml_storage.os, "remove", _raise)
    with pytest.raises(RuntimeError):
        await storage.delete_model("m", data_source="ds")
    assert path not in storage._model_cache


async def test_failed_save_datasource_evicts(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    assert path in storage._datasource_cache
    replacement = DatasourceConfig(name="pg", database="b")
    monkeypatch.setattr(_yaml_storage.yaml, "dump", _raise)
    with pytest.raises(RuntimeError):
        await storage.save_datasource(replacement)
    assert path not in storage._datasource_cache


async def test_failed_delete_datasource_evicts(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    assert path in storage._datasource_cache
    monkeypatch.setattr(_yaml_storage.os, "remove", _raise)
    with pytest.raises(RuntimeError):
        await storage.delete_datasource("pg")
    assert path not in storage._datasource_cache


# ---- models: mutation safety (deep-copy handout) --------------------------


async def test_mutating_returned_model_does_not_poison_cache(storage: YAMLStorage) -> None:
    await storage.save_model(_model())
    first = await storage.get_model("m", data_source="ds")
    first.sql_table = "MUTATED"
    first.columns[0].sql = "MUTATED"  # nested object mutation — shallow copy would leak
    first.columns.clear()
    second = await storage.get_model("m", data_source="ds")
    assert second.sql_table == "public.t"
    assert len(second.columns) == 1
    assert second.columns[0].sql == "id"


# ---- models: migration behavior & cache coherence -------------------------


def _write_legacy_model(storage: YAMLStorage) -> str:
    target_dir = os.path.join(storage.models_dir, "ds")
    os.makedirs(target_dir, exist_ok=True)
    path = os.path.join(target_dir, "m.yaml")
    with open(path, "w") as f:
        _yaml.dump(
            {"version": 1, "name": "m", "sql_table": "public.t", "data_source": "ds"}, f
        )
    return path


async def test_migration_applied_on_first_load(storage: YAMLStorage) -> None:
    path = _write_legacy_model(storage)
    model = await storage.get_model("m", data_source="ds")
    assert model is not None
    assert model.version == _mig.CURRENT_VERSIONS["SlayerModel"]
    with open(path) as f:
        on_disk = _yaml.safe_load(f)
    assert on_disk["version"] == _mig.CURRENT_VERSIONS["SlayerModel"]  # write-back upgraded


async def test_migration_writeback_not_cached_then_coherent(storage: YAMLStorage, monkeypatch) -> None:
    path = _write_legacy_model(storage)
    # First load migrates + writes the file back (new mtime), so the stable-read
    # guard does NOT cache it — the just-parsed bytes no longer match the file.
    await storage.get_model("m", data_source="ds")
    assert path not in storage._model_cache
    # Second load reads the now-current file and caches it coherently.
    await storage.get_model("m", data_source="ds")
    assert storage._model_cache[path][0] == _yaml_storage._stat_key(path)
    # Third load is a hit — no parse.
    loads = _install_counter(monkeypatch, _yaml_storage.yaml, "safe_load")
    await storage.get_model("m", data_source="ds")
    assert loads["n"] == 0


async def test_mid_read_external_edit_not_cached(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_model(_model(sql_table="public.a"))
    path = storage._model_path("ds", "m")
    orig = _yaml_storage.yaml.safe_load
    state = {"fired": False}

    def racing_load(stream):
        data = orig(stream)
        if not state["fired"]:
            state["fired"] = True
            with open(path, "w") as f:
                _yaml.dump(
                    _model(sql_table="public.b").model_dump(mode="json", exclude_none=True), f
                )
            _bump_mtime(path)
        return data

    monkeypatch.setattr(_yaml_storage.yaml, "safe_load", racing_load)
    first = await storage.get_model("m", data_source="ds")
    assert first.sql_table == "public.a"
    assert path not in storage._model_cache  # stable-read guard skipped caching
    second = await storage.get_model("m", data_source="ds")
    assert second.sql_table == "public.b"


# ---- datasources -----------------------------------------------------------


async def test_datasource_hit_skips_parse(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", type="postgres", database="db"))
    loads = _install_counter(monkeypatch, _yaml_storage.yaml, "safe_load")
    assert await storage.get_datasource("pg") is not None
    assert await storage.get_datasource("pg") is not None
    assert loads["n"] == 1


async def test_save_datasource_evicts(storage: YAMLStorage) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    assert path in storage._datasource_cache
    await storage.save_datasource(DatasourceConfig(name="pg", database="b"))
    assert path not in storage._datasource_cache
    assert (await storage.get_datasource("pg")).database == "b"


async def test_delete_datasource_evicts(storage: YAMLStorage) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    assert path in storage._datasource_cache
    await storage.delete_datasource("pg")
    assert path not in storage._datasource_cache
    assert await storage.get_datasource("pg") is None


async def test_datasource_external_edit_invalidates(storage: YAMLStorage) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    with open(path, "w") as f:
        _yaml.dump({"name": "pg", "database": "b"}, f)
    _bump_mtime(path)
    assert (await storage.get_datasource("pg")).database == "b"


async def test_datasource_mid_read_external_edit_not_cached(storage: YAMLStorage, monkeypatch) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    orig = _yaml_storage.yaml.safe_load
    state = {"fired": False}

    def racing_load(stream):
        data = orig(stream)
        if not state["fired"]:
            state["fired"] = True
            with open(path, "w") as f:
                _yaml.dump({"name": "pg", "database": "b"}, f)
            _bump_mtime(path)
        return data

    monkeypatch.setattr(_yaml_storage.yaml, "safe_load", racing_load)
    assert (await storage.get_datasource("pg")).database == "a"
    assert path not in storage._datasource_cache  # stable-read guard skipped caching
    assert (await storage.get_datasource("pg")).database == "b"


async def test_datasource_env_reresolved_on_hit(storage: YAMLStorage, monkeypatch) -> None:
    monkeypatch.setenv("SLAYER_DEV1816_DB", "db1")
    await storage.save_datasource(DatasourceConfig(name="pg", database="${SLAYER_DEV1816_DB}"))
    assert (await storage.get_datasource("pg")).database == "db1"
    monkeypatch.setenv("SLAYER_DEV1816_DB", "db2")
    assert (await storage.get_datasource("pg")).database == "db2"  # cache hit, env re-read


async def test_datasource_mutation_safe(storage: YAMLStorage) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="db"))
    first = await storage.get_datasource("pg")
    first.database = "MUTATED"
    second = await storage.get_datasource("pg")
    assert second.database == "db"


async def test_datasource_invalid_config_wrapped(storage: YAMLStorage) -> None:
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    with open(path, "w") as f:
        _yaml.dump({"name": "pg", "port": "not-an-int"}, f)
    with pytest.raises(ValueError, match="invalid config"):
        await storage.get_datasource("pg")


async def test_datasource_env_resolution_validationerror_wrapped(
    storage: YAMLStorage, monkeypatch
) -> None:
    # Stored config validates unresolved; env substitution then yields an invalid
    # name ('/' is forbidden), so resolve_env_vars()'s reconstruction raises
    # ValidationError — which must stay wrapped as "invalid config" (resolve is
    # inside the try).
    monkeypatch.setenv("SLAYER_DEV1816_NAME", "bad/name")
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    with open(path, "w") as f:
        _yaml.dump({"name": "${SLAYER_DEV1816_NAME}"}, f)
    with pytest.raises(ValueError, match="invalid config"):
        await storage.get_datasource("pg")


async def test_datasource_unresolved_var_propagates(storage: YAMLStorage) -> None:
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    with open(path, "w") as f:
        _yaml.dump({"name": "pg", "database": "${SLAYER_DEV1816_MISSING}"}, f)
    with pytest.raises(ValueError, match="unresolved environment variable"):
        await storage.get_datasource("pg")


# ---- smoke through the JoinSyncStorage wrapper ----------------------------


async def test_wrapped_storage_hands_out_independent_copies() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        wrapped = resolve_storage(tmpdir)
        await wrapped.save_model(_model())
        first = await wrapped.get_model("m", data_source="ds")
        second = await wrapped.get_model("m", data_source="ds")
        assert first is not second
        first.columns[0].sql = "MUTATED"
        third = await wrapped.get_model("m", data_source="ds")
        assert third.columns[0].sql == "id"


async def test_cache_is_per_instance() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        a = YAMLStorage(base_dir=tmpdir)
        b = YAMLStorage(base_dir=tmpdir)
        await a.save_model(_model(sql_table="public.a"))
        await a.get_model("m", data_source="ds")  # primes a only
        path = a._model_path("ds", "m")
        assert path in a._model_cache
        assert path not in b._model_cache
        with open(path, "w") as f:
            _yaml.dump(_model(sql_table="public.b").model_dump(mode="json", exclude_none=True), f)
        _bump_mtime(path)
        # Both instances independently observe the edit via their own stat check.
        assert (await b.get_model("m", data_source="ds")).sql_table == "public.b"
        assert (await a.get_model("m", data_source="ds")).sql_table == "public.b"
