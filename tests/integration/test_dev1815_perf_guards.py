"""DEV-1815 reuse guards: the Jaffle Shop demo is built once and shared.

These lock in the perf-critical reuse so a future change cannot silently
regress to cold-building the demo per server fixture.
"""

from __future__ import annotations

import os
import shutil

import pytest

from slayer.async_utils import run_sync
from slayer.demo.jaffle_shop import DEMO_NAME, TABLE_NAMES

pytestmark = pytest.mark.integration


def _forbid_jafgen(*_args, **_kwargs):
    raise AssertionError("jafgen generate_data must not run when reusing a prebuilt DuckDB")


def test_prepare_demo_from_prebuilt_skips_jafgen_and_localizes_path(
    jaffle_demo_duckdb: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Copying the session DuckDB must skip jafgen yet keep an in-dir datasource path."""
    import slayer.demo.jaffle_shop as jaffle
    from tests.integration import _demo_build

    monkeypatch.setattr(jaffle, "generate_data", _forbid_jafgen)

    args, storage = _demo_build.prepare_demo_storage(prebuilt_duckdb=jaffle_demo_duckdb)

    # The DuckDB lives inside THIS dir (a copy), not the shared template.
    local_db = os.path.join(args.storage, "demo", "jaffle_shop.duckdb")
    assert os.path.exists(local_db)
    assert os.path.abspath(local_db) != os.path.abspath(jaffle_demo_duckdb)

    # Codex #8: the ingested datasource must point at the copy, not the template.
    ds = run_sync(storage.get_datasource(DEMO_NAME))
    assert ds is not None and ds.database is not None
    assert os.path.abspath(ds.database) == os.path.abspath(local_db)


def _forbid_ingest(*_args, **_kwargs):
    raise AssertionError("ingest_datasource must not run when demo models are already warm")


def test_warm_demo_models_skip_reingest(
    jaffle_demo_duckdb: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The notebook harness relies on warm models skipping re-ingestion (DEV-1815 W2)."""
    from slayer.demo.jaffle_shop import ensure_demo_datasource
    from slayer.storage.yaml_storage import YAMLStorage

    base = tmp_path / "warm"
    (base / "demo").mkdir(parents=True)
    shutil.copy2(jaffle_demo_duckdb, base / "demo" / "jaffle_shop.duckdb")
    storage = YAMLStorage(base_dir=str(base))

    # First ingest populates all base models.
    ensure_demo_datasource(
        storage, storage_path=str(base), ingest_models=True, assume_yes=True
    )
    present = set(run_sync(storage.list_models(data_source=DEMO_NAME)))
    assert all(t in present for t in TABLE_NAMES)

    # With models already present, a second call must take the reuse fast-path.
    monkeypatch.setattr(
        "slayer.engine.ingestion.ingest_datasource", _forbid_ingest
    )
    _ds, _models, db_built = ensure_demo_datasource(
        storage, storage_path=str(base), ingest_models=True, assume_yes=True
    )
    assert db_built is False
