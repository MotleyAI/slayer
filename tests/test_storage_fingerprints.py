"""Store fingerprints change on every content write, whatever the timestamps say."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage import content_fingerprint
from slayer.storage import sqlite_storage as sqlite_storage_module
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

SUFFIXES = (".yaml",)


def _model(tag: str) -> SlayerModel:
    return SlayerModel(
        name="m", sql_table="m", data_source="ds", access_tags=[tag],
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )


async def _seeded(storage: StorageBackend) -> StorageBackend:
    await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
    await storage.save_model(_model("aa"), _validate=False)
    return storage


class TestYaml:
    async def test_same_size_rewrite_with_restored_mtime(self, tmp_path: Path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _seeded(storage)
        path = os.path.join(storage.models_dir, "ds", "m.yaml")
        mtime = os.stat(path).st_mtime_ns
        before = await storage.graph_fingerprint()
        await storage.save_model(_model("bb"), _validate=False)
        os.utime(path, ns=(mtime, mtime))
        assert await storage.graph_fingerprint() != before

    async def test_future_mtime_elsewhere_does_not_mask_a_write(self, tmp_path: Path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await _seeded(storage)
        future = time.time() + 3600
        os.utime(os.path.join(storage.datasources_dir, "ds.yaml"), (future, future))
        before = await storage.graph_fingerprint()
        await storage.save_model(_model("bb"), _validate=False)
        assert await storage.graph_fingerprint() != before


class TestRacyClean:
    def test_unchanged_stat_inside_the_racy_window_is_rehashed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        f = tmp_path / "a.yaml"
        f.write_text("one")
        frozen = content_fingerprint.stat_key(str(f))
        monkeypatch.setattr(content_fingerprint, "stat_key", lambda _path: frozen)
        before = content_fingerprint.tree_fingerprint(str(tmp_path), suffixes=SUFFIXES)
        f.write_text("two")
        assert content_fingerprint.tree_fingerprint(str(tmp_path), suffixes=SUFFIXES) != before

    def test_settled_file_is_not_reread(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        (tmp_path / "a.yaml").write_text("one")
        later = time.time_ns() + 10 * content_fingerprint.RACY_NS
        monkeypatch.setattr(content_fingerprint.time, "time_ns", lambda: later)
        reads: list[str] = []
        digest = content_fingerprint.file_digest

        def counting(path: str) -> str:
            reads.append(path)
            return digest(path)

        monkeypatch.setattr(content_fingerprint, "file_digest", counting)
        first = content_fingerprint.tree_fingerprint(str(tmp_path), suffixes=SUFFIXES)
        assert content_fingerprint.tree_fingerprint(str(tmp_path), suffixes=SUFFIXES) == first
        assert len(reads) == 1


class TestSqlite:
    async def test_commit_with_frozen_stat_changes_it(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        db = str(tmp_path / "s.db")
        storage = await _seeded(SQLiteStorage(db_path=db))
        st = os.stat(db)
        monkeypatch.setattr(sqlite_storage_module, "stat_key", lambda _path: (0, 0, 0, 0))
        before = await storage.graph_fingerprint()
        await storage.save_model(_model("bb"), _validate=False)
        os.utime(db, ns=(st.st_atime_ns, st.st_mtime_ns))
        assert await storage.graph_fingerprint() != before
