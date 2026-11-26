"""mtime-keyed dataset/datasource load cache for YAMLStorage."""

import builtins
import io
import os
import tempfile
from pathlib import Path

import pytest
import yaml as _yaml

from slayer.core import models as _core_models
from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, SlayerDataset
from slayer.storage import migrations as _mig
from slayer.storage import yaml_storage as _yaml_storage
from slayer.storage.atomic_write import _atomic_write_text
from slayer.storage.base import resolve_storage
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> YAMLStorage:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


def _dataset(*, name: str = "m", ds: str = "ds", sql_table: str = "public.t") -> SlayerDataset:
    return SlayerDataset(
        name=name,
        sql_table=sql_table,
        data_source=ds,
        fields=[DatasetField(name="id", expression="id", datatype=DataType.STRING, primary_key=True)],
    )


def _install_counter(monkeypatch: pytest.MonkeyPatch, obj, attr: str) -> dict:
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


# ---- datasets: cache hits skip work ---------------------------------------


async def test_hit_skips_parse(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
    await storage.save_dataset(_dataset())
    loads = _install_counter(monkeypatch, _yaml_storage.yaml, "safe_load")
    assert await storage.get_dataset("m", data_source="ds") is not None
    assert await storage.get_dataset("m", data_source="ds") is not None
    assert loads["n"] == 1  # second get was a cache hit


async def test_hit_skips_validate_and_migrate(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
    # A parse runs SlayerModel.model_validate, whose before-validator invokes the
    # migrations pass (_migrate_schema). A cache hit skips both.
    await storage.save_dataset(_dataset())
    validates = _install_counter(monkeypatch, SlayerDataset, "model_validate")
    migrates = _install_counter(monkeypatch, _core_models, "_migrate_schema")
    await storage.get_dataset("m", data_source="ds")  # miss
    v0, m0 = validates["n"], migrates["n"]
    assert v0 >= 1
    assert m0 >= 1
    await storage.get_dataset("m", data_source="ds")  # hit
    assert validates["n"] == v0
    assert migrates["n"] == m0


# ---- datasets: invalidation ------------------------------------------------


# Behaviour only: a read through a warm instance always agrees with the stored files.

SALES_A = """\
version: 0.3.0.dev0
name: sales
datasets:
  - name: m
    source: public.a
    primary_key: [id]
    fields:
      - name: id
        expression: {sql: id}
        datatype: String
"""

#: Where dataset ``m`` lives: its own dataset document (datasource model) or inline in model document ``sales``.
HOMES = ["dataset_document", "model_document"]


def _folder(storage: YAMLStorage) -> Path:
    return Path(storage.base_dir) / "semantic_models" / "ds"


def _home_path(storage: YAMLStorage, home: str) -> Path:
    """The stored file holding dataset ``m``, found by content."""
    hits = []
    for p in sorted(_folder(storage).glob("*.yaml")) if _folder(storage).is_dir() else []:
        doc = _yaml.safe_load(p.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            continue
        if home == "dataset_document" and (doc.get("dataset") or {}).get("name") == "m":
            hits.append(p)
        if home == "model_document" and doc.get("name") == "sales" and "dataset" not in doc:
            hits.append(p)
    assert len(hits) == 1, f"{home} of 'm': {hits}"
    return hits[0]


async def _stored(storage: YAMLStorage, home: str) -> Path:
    """Store dataset ``m`` reading ``public.a`` in ``home``; its file."""
    if home == "dataset_document":
        await storage.save_dataset(_dataset(sql_table="public.a"))
    else:
        await storage.save_semantic_model_text(data_source="ds", name="sales", text=SALES_A)
    return _home_path(storage, home)


async def _table(storage: YAMLStorage) -> str | None:
    dataset = await storage.get_dataset("m", data_source="ds")
    return None if dataset is None else dataset.sql_table


async def _agrees_with_a_fresh_instance(storage: YAMLStorage) -> None:
    assert await _table(storage) == await _table(YAMLStorage(base_dir=storage.base_dir))


def _rewrite_same_size(path: Path) -> None:
    """Edit ``m``'s source from ``public.a`` to ``public.b`` in place (same byte length)."""
    text = path.read_text(encoding="utf-8")
    assert "public.a" in text
    path.write_text(text.replace("public.a", "public.b"), encoding="utf-8")


def _failing_under(monkeypatch: pytest.MonkeyPatch, root: Path, *, ops: tuple[str, ...]) -> None:
    """Every ``os`` ``ops`` call targeting a file under ``root`` raises before it acts."""
    resolved = str(root.resolve())
    for op in ops:
        original = getattr(os, op)

        def run(*args, _original=original, _op=op, **kwargs):
            target = args[-1] if _op in ("replace", "rename") else args[0]
            if str(Path(target).resolve()).startswith(resolved):
                raise RuntimeError("io failed")
            return _original(*args, **kwargs)

        monkeypatch.setattr(os, op, run)


def _on_first_open_of(monkeypatch: pytest.MonkeyPatch, path: Path, act) -> dict:
    """Run ``act(open_args)`` on the first ``open`` of ``path``; ``act`` returns the file object or raises."""
    real_open = builtins.open
    state = {"fired": 0}
    target = str(path.resolve())

    def hooked(file, *args, **kwargs):
        if not state["fired"] and not isinstance(file, int) and str(Path(os.fspath(file)).resolve()) == target:
            state["fired"] += 1
            return act(lambda: real_open(file, *args, **kwargs))
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", hooked)
    monkeypatch.setattr(io, "open", hooked)
    return state


@pytest.mark.parametrize("home", HOMES)
async def test_save_model_evicts(storage: YAMLStorage, home: str) -> None:
    """A save through a warm instance is seen by its next read."""
    await _stored(storage, home)
    assert await _table(storage) == "public.a"
    await storage.save_dataset(_dataset(sql_table="public.b"))
    assert await _table(storage) == "public.b"
    await _agrees_with_a_fresh_instance(storage)


@pytest.mark.parametrize("home", HOMES)
async def test_external_edit_invalidates(storage: YAMLStorage, home: str) -> None:
    path = await _stored(storage, home)
    assert await _table(storage) == "public.a"
    size_before = path.stat().st_size
    _rewrite_same_size(path)
    _bump_mtime(str(path))
    assert path.stat().st_size == size_before
    assert await _table(storage) == "public.b"


@pytest.mark.parametrize("home", HOMES)
async def test_same_mtime_size_rewrite_invalidated_by_ctime(storage: YAMLStorage, home: str) -> None:
    path = await _stored(storage, home)
    assert await _table(storage) == "public.a"
    st = path.stat()
    _rewrite_same_size(path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns))
    after = path.stat()
    assert (after.st_mtime_ns, after.st_size) == (st.st_mtime_ns, st.st_size)
    assert await _table(storage) == "public.b"


@pytest.mark.parametrize("home", HOMES)
async def test_external_delete_evicts_model_cache(storage: YAMLStorage, home: str) -> None:
    path = await _stored(storage, home)
    assert await _table(storage) == "public.a"
    os.remove(path)
    assert await _table(storage) is None
    await _agrees_with_a_fresh_instance(storage)


@pytest.mark.parametrize("home", HOMES)
async def test_model_delete_race_at_stat_returns_none_and_evicts(
        storage: YAMLStorage, home: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """The file deleted at its first stat after a warm read reads as absent, then stays absent."""
    path = await _stored(storage, home)
    assert await _table(storage) == "public.a"
    real_stat = os.stat
    target = str(path.resolve())
    state = {"fired": 0}

    def racing_stat(p, *args, **kwargs):
        if not state["fired"] and not isinstance(p, int) and os.path.realpath(os.fspath(p)) == target:
            state["fired"] += 1
            os.remove(target)
            raise FileNotFoundError(target)
        return real_stat(p, *args, **kwargs)

    monkeypatch.setattr(os, "stat", racing_stat)
    result = await _table(storage)
    assert result is None if state["fired"] else result == "public.a"
    monkeypatch.setattr(os, "stat", real_stat)
    if path.exists():
        os.remove(target)
    assert await _table(storage) is None
    await _agrees_with_a_fresh_instance(storage)


@pytest.mark.parametrize("home", HOMES)
async def test_model_delete_race_at_open_returns_none_and_evicts(
        storage: YAMLStorage, home: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """The file deleted at its open after an external edit reads as absent, then stays absent."""
    path = await _stored(storage, home)
    assert await _table(storage) == "public.a"
    _rewrite_same_size(path)
    _bump_mtime(str(path))

    def deleted(_open):
        os.remove(path)
        raise FileNotFoundError(str(path))

    state = _on_first_open_of(monkeypatch, path, deleted)
    assert await _table(storage) is None
    assert state["fired"] == 1
    monkeypatch.undo()
    assert await _table(storage) is None
    await _agrees_with_a_fresh_instance(storage)


async def test_delete_model_evicts(storage: YAMLStorage) -> None:
    await _stored(storage, "dataset_document")
    assert await _table(storage) == "public.a"
    await storage.delete_dataset("m", data_source="ds")
    assert await _table(storage) is None
    await _agrees_with_a_fresh_instance(storage)


@pytest.mark.parametrize("home", HOMES)
async def test_failed_save_evicts_before_write(storage: YAMLStorage, home: str,
                                               monkeypatch: pytest.MonkeyPatch) -> None:
    """After a failed save, an external edit is still seen by the next read."""
    path = await _stored(storage, home)
    assert await _table(storage) == "public.a"
    with monkeypatch.context() as patch:
        _failing_under(patch, _folder(storage), ops=("replace", "rename"))
        with pytest.raises(RuntimeError):
            await storage.save_dataset(_dataset(sql_table="public.c"))
    _rewrite_same_size(path)
    _bump_mtime(str(path))
    assert await _table(storage) == "public.b"


@pytest.mark.parametrize("home", HOMES)
async def test_failed_update_column_sampled_evicts(storage: YAMLStorage, home: str,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    path = await _stored(storage, home)
    before = path.read_bytes()
    assert await _table(storage) == "public.a"
    with monkeypatch.context() as patch:
        _failing_under(patch, _folder(storage), ops=("replace", "rename"))
        with pytest.raises(RuntimeError):
            await storage.update_field_sampled(data_source="ds", dataset_name="m", field_name="id",
                                               sampled="x", sampled_values=None, distinct_count=None)
    assert path.read_bytes() == before
    reloaded = await storage.get_dataset("m", data_source="ds")
    assert reloaded is not None
    assert reloaded.fields[0].sampled is None
    await _agrees_with_a_fresh_instance(storage)


async def test_failed_delete_model_evicts(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
    """After a failed delete, the warm instance reads what a fresh one reads."""
    await _stored(storage, "dataset_document")
    assert await _table(storage) == "public.a"
    with monkeypatch.context() as patch:
        _failing_under(patch, _folder(storage), ops=("remove", "unlink"))
        with pytest.raises(RuntimeError):
            await storage.delete_dataset("m", data_source="ds")
    await _agrees_with_a_fresh_instance(storage)


@pytest.mark.parametrize("home", HOMES)
async def test_failed_save_after_warm_cache_serves_preserved_file(
    storage: YAMLStorage, home: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed atomic save changes no file; the next read serves the preserved content."""
    path = await _stored(storage, home)
    before = path.read_bytes()
    assert await _table(storage) == "public.a"
    with monkeypatch.context() as patch:
        _failing_under(patch, _folder(storage), ops=("replace", "rename"))
        with pytest.raises(RuntimeError):
            await storage.save_dataset(_dataset(sql_table="public.b"))
    assert path.read_bytes() == before
    assert await _table(storage) == "public.a"


@pytest.mark.parametrize("home", HOMES)
async def test_external_atomic_replace_invalidates_cache(storage: YAMLStorage, home: str) -> None:
    """Another process replacing the file atomically (a fresh inode) is seen by the next read."""
    path = await _stored(storage, home)
    assert await _table(storage) == "public.a"
    _atomic_write_text(path=str(path), text=path.read_text(encoding="utf-8").replace("public.a", "public.bbbb"))
    assert await _table(storage) == "public.bbbb"


@pytest.mark.parametrize("home", HOMES)
async def test_mid_read_external_edit_not_cached(storage: YAMLStorage, home: str,
                                                 monkeypatch: pytest.MonkeyPatch) -> None:
    """A file replaced while a read holds it open is re-read next time."""
    path = await _stored(storage, home)
    _bump_mtime(str(path))  # a cold or stale entry: the next read opens the file
    edited = path.read_text(encoding="utf-8").replace("public.a", "public.b")

    def replaced_after_open(open_file):
        handle = open_file()
        _atomic_write_text(path=str(path), text=edited)
        return handle

    fresh = YAMLStorage(base_dir=storage.base_dir)
    state = _on_first_open_of(monkeypatch, path, replaced_after_open)
    first = await _table(fresh)
    assert state["fired"] == 1
    assert first == "public.a"
    monkeypatch.undo()
    assert await _table(fresh) == "public.b"


@pytest.mark.parametrize("home", HOMES)
async def test_cache_is_per_instance(home: str) -> None:
    """Two instances over one store each see the other's and external writes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        a = YAMLStorage(base_dir=tmpdir)
        b = YAMLStorage(base_dir=tmpdir)
        path = await _stored(a, home)
        assert await _table(a) == "public.a"
        assert await _table(b) == "public.a"
        _rewrite_same_size(path)
        _bump_mtime(str(path))
        assert await _table(b) == "public.b"
        assert await _table(a) == "public.b"
        await a.save_dataset(_dataset(sql_table="public.c"))
        assert await _table(b) == "public.c"


async def test_external_delete_evicts_datasource_cache(storage: YAMLStorage) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    assert path in storage._datasource_cache
    os.remove(path)  # external delete
    assert await storage.get_datasource("pg") is None
    assert path not in storage._datasource_cache


def _stat_key_raises_once(monkeypatch: pytest.MonkeyPatch):
    """Make the next stat_key call raise FileNotFoundError (file deleted between
    the existence check and the stat), then behave normally."""
    orig = _yaml_storage.stat_key
    state = {"fired": False}

    def flaky(path):
        if not state["fired"]:
            state["fired"] = True
            raise FileNotFoundError(path)
        return orig(path)

    monkeypatch.setattr(_yaml_storage, "stat_key", flaky)


async def test_datasource_delete_race_at_stat_returns_none_and_evicts(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    assert path in storage._datasource_cache
    _stat_key_raises_once(monkeypatch)
    assert await storage.get_datasource("pg") is None
    assert path not in storage._datasource_cache


def _open_raises_for(monkeypatch: pytest.MonkeyPatch, target_path):
    """Make open(target_path) raise FileNotFoundError (file deleted between the
    stat and the open); other opens behave normally."""
    real_open = open

    def flaky(file, *args, **kwargs):
        if file == target_path:
            raise FileNotFoundError(file)
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(_yaml_storage, "open", flaky, raising=False)


async def test_datasource_delete_race_at_open_returns_none_and_evicts(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    storage._datasource_cache[path] = ((0, 0, 0, 0), storage._datasource_cache[path][1])
    _open_raises_for(monkeypatch, path)
    assert await storage.get_datasource("pg") is None
    assert path not in storage._datasource_cache


async def test_update_column_sampled_evicts(storage: YAMLStorage) -> None:
    await storage.save_dataset(_dataset())
    await storage.get_dataset("m", data_source="ds")  # populate
    await storage.update_field_sampled(
        data_source="ds",
        dataset_name="m",
        field_name="id",
        sampled="s-val",
        sampled_values=["a"],
        distinct_count=1,
    )
    reloaded = await storage.get_dataset("m", data_source="ds")
    assert reloaded.fields[0].sampled == "s-val"


def _raise(*args, **kwargs):
    raise RuntimeError("io failed")


async def test_failed_save_datasource_evicts(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    assert path in storage._datasource_cache
    replacement = DatasourceConfig(name="pg", database="b")
    monkeypatch.setattr(_yaml_storage.yaml, "dump", _raise)
    with pytest.raises(RuntimeError):
        await storage.save_datasource(replacement)
    assert path not in storage._datasource_cache


async def test_failed_delete_datasource_evicts(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    assert path in storage._datasource_cache
    monkeypatch.setattr(_yaml_storage.os, "remove", _raise)
    with pytest.raises(RuntimeError):
        await storage.delete_datasource("pg")
    assert path not in storage._datasource_cache


# ---- atomic-write interplay (#323) -----------------------------------------


async def test_failed_save_datasource_after_warm_cache_serves_preserved_file(
    storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch,
) -> None:
    await storage.save_datasource(DatasourceConfig(name="pg", database="a"))
    await storage.get_datasource("pg")  # populate
    path = os.path.join(storage.datasources_dir, "pg.yaml")
    monkeypatch.setattr("slayer.storage.atomic_write.yaml.dump", _raise)
    replacement = DatasourceConfig(name="pg", database="b")
    with pytest.raises(RuntimeError):
        await storage.save_datasource(replacement)
    assert (await storage.get_datasource("pg")).database == "a"
    assert path in storage._datasource_cache


# ---- datasets: mutation safety (deep-copy handout) ------------------------


async def test_mutating_returned_model_does_not_poison_cache(storage: YAMLStorage) -> None:
    await storage.save_dataset(_dataset())
    first = await storage.get_dataset("m", data_source="ds")
    first.sql_table = "MUTATED"
    first.fields[0].expression = "MUTATED"  # nested object mutation — shallow copy would leak
    first.fields.clear()
    second = await storage.get_dataset("m", data_source="ds")
    assert second.sql_table == "public.t"
    assert len(second.fields) == 1
    assert second.fields[0].expression == "id"


# ---- datasets: migration behavior & cache coherence -----------------------


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
    with open(path) as f:
        before = f.read()
    model = await storage.get_dataset("m", data_source="ds")
    assert model is not None
    assert model.version == _mig.CURRENT_VERSIONS["SlayerModel"]
    with open(path) as f:
        assert f.read() == before


async def test_an_old_document_is_not_cached(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write_legacy_model(storage)
    # An older document upgrades in memory on every load until it is stored upgraded.
    await storage.get_dataset("m", data_source="ds")
    assert path not in storage._dataset_cache
    loads = _install_counter(monkeypatch, _yaml_storage.yaml, "safe_load")
    await storage.get_dataset("m", data_source="ds")
    assert loads["n"] == 1


# ---- datasources -----------------------------------------------------------


async def test_datasource_hit_skips_parse(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
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


async def test_datasource_mid_read_external_edit_not_cached(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
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


async def test_datasource_env_reresolved_on_hit(storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch) -> None:
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
    storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch
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


# ---- smoke through the resolve_storage factory path -----------------------


async def test_factory_storage_hands_out_independent_copies() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        wrapped = resolve_storage(tmpdir)
        await wrapped.save_dataset(_dataset())
        first = await wrapped.get_dataset("m", data_source="ds")
        second = await wrapped.get_dataset("m", data_source="ds")
        assert first is not second
        first.fields[0].expression = "MUTATED"
        third = await wrapped.get_dataset("m", data_source="ds")
        assert third.fields[0].expression == "id"

