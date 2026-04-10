"""Integration tests that execute all example notebooks end-to-end.

Each notebook under docs/examples/ is run via nbclient. Success means
the notebook completes without raising any exceptions.
"""

from pathlib import Path

import pytest

nbclient = pytest.importorskip("nbclient")
nbformat = pytest.importorskip("nbformat")

pytestmark = pytest.mark.integration

EXAMPLES_DIR = Path(__file__).resolve().parent.parent.parent / "docs" / "examples"

# Discover all .ipynb files, excluding checkpoints
_NOTEBOOKS = sorted(
    p for p in EXAMPLES_DIR.rglob("*.ipynb")
    if ".ipynb_checkpoints" not in str(p)
)


@pytest.fixture(params=_NOTEBOOKS, ids=[str(p.relative_to(EXAMPLES_DIR)) for p in _NOTEBOOKS])
def notebook_path(request):
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
