"""`slayer import-cube` on JavaScript configs + --ignore-required-meta (DEV-1730)."""

import asyncio
import os
import sys
from contextlib import contextmanager

from slayer.cli import main as cli_main
from slayer.core.query import extract_model_variables
from slayer.storage.yaml_storage import YAMLStorage

JS_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "cube_js")


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


def _get_model(storage_dir, name):
    storage = YAMLStorage(base_dir=str(storage_dir))
    return asyncio.new_event_loop().run_until_complete(
        storage.get_model(name, data_source="cube_ds")
    )


def test_import_cube_discovers_js_files(tmp_path):
    storage_dir = tmp_path / "store"
    code = _run(
        "import-cube", JS_FIXTURE_DIR,
        "--datasource", "cube_ds",
        "--storage", str(storage_dir),
    )
    assert code == 0
    model = _get_model(storage_dir, "RrDrivers")
    assert model is not None
    # default honors meta.required -> the date vars are required (bare in SQL).
    v = extract_model_variables(model)
    assert "fulfillment_date_from" in v.required
    assert set(v.optional) >= {"brand", "market", "category"}


def test_ignore_required_meta_flag_makes_all_optional(tmp_path):
    storage_dir = tmp_path / "store"
    code = _run(
        "import-cube", JS_FIXTURE_DIR,
        "--datasource", "cube_ds",
        "--storage", str(storage_dir),
        "--ignore-required-meta",
    )
    assert code == 0
    model = _get_model(storage_dir, "RrDrivers")
    # With the flag, the scalar-position arrow becomes an optional block.
    assert "{? '{fulfillment_date_from}' ?}::TIMESTAMP" in model.sql
