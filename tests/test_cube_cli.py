"""Tests for the `slayer import-cube` CLI (slayer/cli.py).

DEV-1608 §11. Offline: writes SLayer models to storage + a JSON report. No
datasource connection required.
"""

import asyncio
import json
import os
import sys
from contextlib import contextmanager

from slayer.cli import main as cli_main
from slayer.storage.yaml_storage import YAMLStorage

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "cube_project")


@contextmanager
def _argv(*argv: str):
    original = sys.argv
    sys.argv = ["slayer", *argv]
    try:
        yield
    finally:
        sys.argv = original


def _run(*argv: str) -> int:
    with _argv(*argv):
        try:
            cli_main()
        except SystemExit as exc:  # NOSONAR — capture CLI exit code
            return int(exc.code or 0)
    return 0


def test_import_cube_writes_models_and_report(tmp_path):
    storage_dir = tmp_path / "store"
    code = _run(
        "import-cube", FIXTURE,
        "--datasource", "cube_ds",
        "--storage", str(storage_dir),
    )
    assert code == 0

    # Models persisted, offline (no datasource ever created/connected).
    storage = YAMLStorage(base_dir=str(storage_dir))
    names = asyncio.new_event_loop().run_until_complete(storage.list_models())
    assert "orders" in names
    assert "customers" in names
    assert "orders_overview" in names

    # JSON report written next to storage.
    report_path = storage_dir / "cube_import_report.json"
    assert report_path.exists()
    report = json.loads(report_path.read_text())
    assert "issues" in report
    assert report["model_count"] >= 3


def test_import_cube_survives_save_failure(tmp_path, monkeypatch):
    # A save_model failure on one model must not abort the run — the report is
    # still written ("report, don't crash").
    import slayer.storage.yaml_storage as ys

    async def _boom(self, model):
        raise RuntimeError("save exploded")

    monkeypatch.setattr(ys.YAMLStorage, "save_model", _boom)
    storage_dir = tmp_path / "store"
    code = _run("import-cube", FIXTURE, "--datasource", "cube_ds", "--storage", str(storage_dir))
    assert code == 0
    assert (storage_dir / "cube_import_report.json").exists()


def test_import_cube_report_honors_models_dir(tmp_path):
    # With --models-dir (and no --storage) the report lands next to the models,
    # matching _resolve_storage's resolution chain.
    models_dir = tmp_path / "mstore"
    code = _run(
        "import-cube", FIXTURE,
        "--datasource", "cube_ds",
        "--models-dir", str(models_dir),
    )
    assert code == 0
    assert (models_dir / "cube_import_report.json").exists()


def test_import_cube_report_path_override(tmp_path):
    storage_dir = tmp_path / "store"
    report_path = tmp_path / "custom_report.json"
    code = _run(
        "import-cube", FIXTURE,
        "--datasource", "cube_ds",
        "--storage", str(storage_dir),
        "--report", str(report_path),
    )
    assert code == 0
    assert report_path.exists()
