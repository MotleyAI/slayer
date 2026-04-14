"""Integration tests that execute all example notebooks end-to-end.

Each notebook under docs/examples/ is run via nbclient. Success means
the notebook completes without raising any exceptions.

The Jaffle Shop database is generated once per test session (slow ~1-2 min).
The models directory is cleaned before each notebook to prevent stale
cross-notebook state (custom models created by one notebook shouldn't
leak into another).
"""

import shutil
from pathlib import Path

import pytest

nbclient = pytest.importorskip("nbclient")
nbformat = pytest.importorskip("nbformat")

pytestmark = pytest.mark.integration

EXAMPLES_DIR = Path(__file__).resolve().parent.parent.parent / "docs" / "examples"
JAFFLE_DATA_DIR = EXAMPLES_DIR / "jaffle_data"
JAFFLE_DB_PATH = JAFFLE_DATA_DIR / "jaffle_shop.duckdb"
JAFFLE_MODELS_DIR = JAFFLE_DATA_DIR / "slayer_models"

# Discover all .ipynb files, excluding checkpoints
_NOTEBOOKS = sorted(
    p for p in EXAMPLES_DIR.rglob("*.ipynb")
    if ".ipynb_checkpoints" not in str(p)
)


@pytest.fixture(scope="session", autouse=True)
def _ensure_jaffle_db():
    """Generate the Jaffle Shop DuckDB once for the entire test session."""
    if JAFFLE_DB_PATH.exists():
        return  # Reuse existing DB

    import sys
    sys.path.insert(0, str(JAFFLE_DATA_DIR))
    import duckdb
    from ingest_jaffle_shop import SCHEMA_FILE, create_schema, generate_data, load_data

    data_dir = generate_data(output_dir=str(JAFFLE_DATA_DIR), years=3)
    conn = duckdb.connect(str(JAFFLE_DB_PATH))
    create_schema(conn, SCHEMA_FILE)
    load_data(conn, data_dir)
    conn.close()


@pytest.fixture(params=_NOTEBOOKS, ids=[str(p.relative_to(EXAMPLES_DIR)) for p in _NOTEBOOKS])
def notebook_path(request):
    # Clean models before each notebook so custom models from one
    # notebook don't leak into another (e.g., order_items_custom).
    if JAFFLE_MODELS_DIR.exists():
        shutil.rmtree(JAFFLE_MODELS_DIR)
    return request.param


def test_notebook_runs_without_errors(notebook_path):
    """Execute the notebook and assert it completes without errors."""
    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    client = nbclient.NotebookClient(
        nb,
        timeout=600,
        kernel_name="python3",
        resources={"metadata": {"path": str(notebook_path.parent)}},
    )
    client.execute()
