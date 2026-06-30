"""Integration tests that execute all example notebooks end-to-end.

Each notebook under docs/examples/ is run via nbclient. Success means
the notebook completes without raising any exceptions.

The Jaffle Shop database is generated once per test session (slow ~1-2 min).
The models directory is cleaned before each notebook to prevent stale
cross-notebook state (custom models created by one notebook shouldn't
leak into another).
"""

import shutil
import socket
from pathlib import Path

import pytest

nbclient = pytest.importorskip("nbclient")
nbformat = pytest.importorskip("nbformat")

pytestmark = pytest.mark.integration

EXAMPLES_DIR = Path(__file__).resolve().parent.parent.parent / "docs" / "examples"
JAFFLE_DATA_DIR = EXAMPLES_DIR / "jaffle_data"
JAFFLE_DB_PATH = JAFFLE_DATA_DIR / "demo" / "jaffle_shop.duckdb"
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

    from slayer.demo.jaffle_shop import build_jaffle_shop

    JAFFLE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        build_jaffle_shop(db_path=str(JAFFLE_DB_PATH), years=3)
    except (FileNotFoundError, RuntimeError) as e:
        pytest.skip(f"Jaffle shop prerequisite missing: {e}")


@pytest.fixture(params=_NOTEBOOKS, ids=[str(p.relative_to(EXAMPLES_DIR)) for p in _NOTEBOOKS])
def notebook_path(request):
    # Clean models before each notebook so custom models from one
    # notebook don't leak into another (e.g., order_items_custom).
    if JAFFLE_MODELS_DIR.exists():
        shutil.rmtree(JAFFLE_MODELS_DIR)
    return request.param


# The dbt MetricFlow notebook bootstraps by shallow-cloning an upstream GitHub
# repo on first run. When that bootstrap can't complete offline, skip rather
# than fail. The helper writes a `.complete` marker only after a fully built
# cache, so its presence is the authoritative "network-free from here" signal;
# a bare/partial clone dir is not enough.
_METRICFLOW_NB_DIR = "10_dbt_metricflow"


def _github_reachable(host: str = "github.com", port: int = 443, timeout: float = 5.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def test_notebook_runs_without_errors(notebook_path):
    """Execute the notebook and assert it completes without errors."""
    is_metricflow = _METRICFLOW_NB_DIR in notebook_path.parts
    if is_metricflow:
        complete_marker = notebook_path.parent / ".cache" / ".complete"
        if not complete_marker.exists() and not _github_reachable():
            pytest.skip("GitHub unreachable; cannot bootstrap the MetricFlow notebook")

    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    client = nbclient.NotebookClient(
        nb,
        timeout=600,
        kernel_name="python3",
        resources={"metadata": {"path": str(notebook_path.parent)}},
    )
    try:
        client.execute()
    except nbclient.exceptions.CellExecutionError as exc:
        # A reachable socket but a failing `git fetch` (or a stale/partial cache)
        # surfaces as MetricFlowDemoError mid-run; skip when GitHub is unreachable
        # rather than reporting a bootstrap failure as a notebook bug.
        if is_metricflow and "MetricFlowDemoError" in str(exc) and not _github_reachable():
            pytest.skip(f"MetricFlow notebook could not bootstrap offline: {exc}")
        raise
