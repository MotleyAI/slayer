"""DEV-1815 reuse guards: the Jaffle Shop demo is built once and shared.

These lock in the perf-critical reuse so a future change cannot silently
regress to cold-building the demo per server fixture.
"""

from __future__ import annotations

import os
import shutil

import pytest

import slayer.demo.jaffle_shop as jaffle
from slayer.async_utils import run_sync
from slayer.demo.jaffle_shop import DEMO_NAME, TABLE_NAMES, ensure_demo_datasource
from slayer.storage.yaml_storage import YAMLStorage

from tests.integration import _demo_build

pytestmark = pytest.mark.integration


def _forbid_jafgen(*_args, **_kwargs):
    raise AssertionError("jafgen generate_data must not run when reusing a prebuilt DuckDB")


def test_prepare_demo_from_prebuilt_skips_jafgen_and_localizes_path(
    jaffle_demo_duckdb: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Copying the session DuckDB must skip jafgen yet keep an in-dir datasource path."""
    monkeypatch.setattr(jaffle, "generate_data", _forbid_jafgen)

    args, storage = _demo_build.prepare_demo_storage(
        base_dir=str(tmp_path / "prep"), prebuilt_duckdb=jaffle_demo_duckdb
    )

    # The DuckDB lives inside THIS dir (a copy), not the shared template.
    local_db = os.path.join(args.storage, "demo", "jaffle_shop.duckdb")
    assert os.path.exists(local_db)
    assert os.path.abspath(local_db) != os.path.abspath(jaffle_demo_duckdb)

    # Codex #8: the ingested datasource must point at the copy, not the template.
    ds = run_sync(storage.get_datasource(DEMO_NAME))
    assert ds is not None
    assert ds.database is not None
    assert os.path.abspath(ds.database) == os.path.abspath(local_db)


def test_warm_demo_setup_rebuilds_and_writes_nothing(
    jaffle_demo_duckdb: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second demo setup over warm models reconciles in place: no DuckDB rebuild, no model write."""
    base = tmp_path / "warm"
    (base / "demo").mkdir(parents=True)
    shutil.copy2(jaffle_demo_duckdb, base / "demo" / "jaffle_shop.duckdb")
    storage = YAMLStorage(base_dir=str(base))

    # First setup populates all base models.
    ensure_demo_datasource(
        storage, storage_path=str(base), ingest_models=True, assume_yes=True
    )
    present = set(run_sync(storage.list_models(data_source=DEMO_NAME)))
    assert all(t in present for t in TABLE_NAMES)

    async def _forbid_save(*_args, **_kwargs):
        raise AssertionError("a warm demo setup must not rewrite any model")

    monkeypatch.setattr(jaffle, "generate_data", _forbid_jafgen)
    monkeypatch.setattr(storage, "save_model", _forbid_save)
    _ds, models, db_built = ensure_demo_datasource(
        storage, storage_path=str(base), ingest_models=True, assume_yes=True
    )
    assert db_built is False
    assert {m.name for m in models} == set(TABLE_NAMES)
