"""Integration test that executes the Jaffle Shop Jupyter notebook end-to-end."""

import os

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("jafgen")
pytest.importorskip("nbclient")
pytest.importorskip("nbformat")

import nbclient
import nbformat


NOTEBOOK_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "examples", "jaffle_shop_ingest.ipynb")


@pytest.mark.integration
def test_notebook_runs_without_errors():
    """Execute every cell in the notebook and verify no exceptions are raised."""
    with open(NOTEBOOK_PATH) as f:
        nb = nbformat.read(fp=f, as_version=4)

    client = nbclient.NotebookClient(
        nb=nb,
        timeout=600,
        kernel_name="python3",
        resources={"metadata": {"path": os.path.dirname(NOTEBOOK_PATH)}},
    )
    client.execute()

    # Verify all code cells executed successfully
    for cell in nb.cells:
        if cell.cell_type == "code":
            for output in cell.get("outputs", []):
                assert output.get("output_type") != "error", (
                    f"Cell raised an error:\n{output.get('evalue', '')}\n{output.get('traceback', '')}"
                )
