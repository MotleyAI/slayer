"""Integration tests that execute all example notebooks end-to-end.

Each notebook under docs/examples/ is run via nbclient. Success means
the notebook completes without raising any exceptions.

The Jaffle Shop database is generated once per test session (slow ~1-2 min).
The models directory is cleaned before each notebook to prevent stale
cross-notebook state (custom models created by one notebook shouldn't
leak into another).
"""

import re
import shutil
import socket
import sys
from pathlib import Path

import pytest

from slayer.async_utils import run_sync
from slayer.demo.jaffle_shop import (
    DEMO_NAME,
    TABLE_NAMES,
    build_jaffle_shop,
    ensure_demo_datasource,
)
from slayer.storage.yaml_storage import YAMLStorage

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

    JAFFLE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        build_jaffle_shop(db_path=str(JAFFLE_DB_PATH), years=3)
    except (FileNotFoundError, RuntimeError) as e:
        pytest.skip(f"Jaffle shop prerequisite missing: {e}")


# Notebooks whose POINT is to demonstrate initial ingestion — they must run on a
# CLEAN models dir so they exercise the full auto-ingest path. DEV-1815 removed
# the dedicated test_jaffle_shop_notebook.py, so this harness now carries that
# cold-ingest coverage. Every other notebook gets the prebuilt base models
# restored (below) so it skips re-ingestion.
_INGEST_DEMO_NOTEBOOKS = {
    "03_auto_ingest/auto_ingest_nb.ipynb",
}


@pytest.fixture(scope="session")
def _jaffle_models_template(_ensure_jaffle_db, tmp_path_factory) -> Path:
    """Ingest the base Jaffle models once and snapshot ``slayer_models/`` (DEV-1815).

    Built fresh (not from a possibly-stale checkout dir) and validated to contain
    every demo table before snapshotting, so consumer notebooks can restore it and
    hit ``ensure_demo_datasource``'s reuse fast-path instead of re-ingesting.
    """
    if JAFFLE_MODELS_DIR.exists():
        shutil.rmtree(JAFFLE_MODELS_DIR)
    storage = YAMLStorage(base_dir=str(JAFFLE_MODELS_DIR))
    ensure_demo_datasource(
        storage,
        storage_path=str(JAFFLE_DATA_DIR),
        years=3,
        ingest_models=True,
        assume_yes=True,
    )
    present = set(run_sync(storage.list_models(data_source=DEMO_NAME)))
    missing = [t for t in TABLE_NAMES if t not in present]
    if missing:
        pytest.skip(f"Jaffle models template incomplete: missing {missing}")

    snapshot = tmp_path_factory.mktemp("jaffle-models-template")
    shutil.copytree(JAFFLE_MODELS_DIR, snapshot, dirs_exist_ok=True)
    return snapshot


# Notebooks expected to fail under the current typed-pipeline gaps.
# Map: notebook path relative to EXAMPLES_DIR → Linear issue + reason.
# Re-enable a notebook by removing its entry once the cited issue lands.
_KNOWN_FAILING_NOTEBOOKS = {
    # DEV-1474 (cross-model partition in time_shift CTEs) landed in DEV-1711
    # Stage 7 — the 04_time QoQ-by-store cell now runs, so that notebook is
    # un-skipped. 09_lightning_talk stays skipped for a DIFFERENT, downstream
    # reason: its hero cell issues ONE query with TWO time_shift transforms
    # (change_pct + an explicit time_shift), which collide on the CTE name
    # `shifted__time_shift_inner` — the DEV-1692 de-collision gap owned by
    # Stage 9 (same gap as the 13_osi_import notebooks below).
    "09_lightning_talk/lightning_talk_nb.ipynb": (
        "DEV-1713: DEV-1692 duplicate time_shift CTE name "
        "(`Duplicate CTE name \"shifted__time_shift_inner\"`) — the hero query "
        "combines change_pct(order_total:sum) with an explicit "
        "time_shift(order_total:sum, -1, 'month') in one query, so two shifted "
        "CTEs are emitted under the same name. DEV-1474's cross-model partition "
        "(the Stage-7 blocker) is fixed; this is the Stage-9 collision gap."
    ),
    # DEV-1704 Stage-0 parity gaps surfaced by the integration notebook run.
    "12_query_cache/query_cache_nb.ipynb": (
        "DEV-1715: the DEV-1587 per-query cache is not yet wired into the "
        "typed pipeline (deferred from Stage 0)."
    ),
}


@pytest.fixture(params=_NOTEBOOKS, ids=[str(p.relative_to(EXAMPLES_DIR)) for p in _NOTEBOOKS])
def notebook_path(request, _jaffle_models_template):
    # Wipe models before each notebook so custom models from one notebook don't
    # leak into another (e.g., order_items_custom). For everything but the
    # ingest-demo notebooks, restore the prebuilt base models so the notebook's
    # ensure_jaffle_shop() call reuses them instead of re-ingesting (DEV-1815).
    if JAFFLE_MODELS_DIR.exists():
        shutil.rmtree(JAFFLE_MODELS_DIR)
    rel = str(request.param.relative_to(EXAMPLES_DIR))
    if rel not in _INGEST_DEMO_NOTEBOOKS:
        shutil.copytree(_jaffle_models_template, JAFFLE_MODELS_DIR)
    return request.param


# The dbt MetricFlow notebook bootstraps by shallow-cloning an upstream GitHub
# repo on first run. When that bootstrap can't complete offline, skip rather
# than fail. The helper writes a `.complete` marker only after a fully built
# cache, so its presence is the authoritative "network-free from here" signal;
# a bare/partial clone dir is not enough.
_METRICFLOW_NB_DIR = "11_dbt_metricflow"


def _github_reachable(host: str = "github.com", port: int = 443, timeout: float = 5.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _bootstrap_failure_is_transient(error_text: str) -> bool:
    """True if a MetricFlow bootstrap error reflects a transient network/server
    problem (GitHub 5xx/429, DNS, dropped connection) rather than a deterministic
    one (bad pin SHA, missing CSVs). A reachable socket does not guarantee a clone
    succeeds — GitHub can accept the connection and still answer 503 — so the skip
    guard consults this in addition to :func:`_github_reachable`.

    Reuses the setup helper's classifier (imported lazily, mirroring the in-fixture
    ``build_jaffle_shop`` import above) so the retry loop and skip guard agree on
    what counts as transient. If the helper can't be imported, err toward *not*
    transient so a genuine failure is never silently skipped.
    """
    metricflow_dir = EXAMPLES_DIR / _METRICFLOW_NB_DIR
    if str(metricflow_dir) not in sys.path:
        sys.path.insert(0, str(metricflow_dir))
    try:
        from setup_metricflow import _is_transient_git_error  # ALLOW(import-not-top): sys.path set above at call time
    except ImportError:
        return False
    return _is_transient_git_error(error_text)


# The DuckDB example notebooks read a CSV live over httpfs; DuckDB also
# auto-installs the httpfs extension from its repo on a clean machine. The CDN
# host serving the CSV is always needed, so the pre-run probe gates on it (over
# 443). A missing extension repo (served over 80) surfaces only mid-run and is
# caught by the transient classifier below, which names both hosts. Mirrors the
# MetricFlow guard: skip (never fail) when the network is down, so offline runs
# stay green.
_DUCKDB_NB_DIR = "15_duckdb"
_DUCKDB_DATA_HOST = "cdn.jsdelivr.net"
# Remote hosts the DuckDB notebooks reach: the CSV CDN, the httpfs extension
# repo, and (CLI notebook) the DuckDB CLI installer + version endpoint.
_DUCKDB_REMOTE_HOSTS = (
    _DUCKDB_DATA_HOST,
    "extensions.duckdb.org",
    "install.duckdb.org",
    "duckdb.org",
)

# Substrings marking a mid-run failure as a transient network/server hiccup
# reaching one of those hosts. Matched case-insensitively and only when the
# error also names a remote host, so genuine query / ingestion bugs still fail
# loudly.
_DUCKDB_TRANSIENT_SIGNATURES = (
    r"http (?:429|5\d\d)",
    r"could not resolve host",
    r"temporary failure in name resolution",
    r"name or service not known",
    r"connection reset",
    r"connection refused",
    r"connection timed out",
    r"timed out",
    r"broken pipe",
    r"could not establish",
    r"failed to (?:connect|download)",
)


def _duckdb_data_host_reachable() -> bool:
    return _github_reachable(host=_DUCKDB_DATA_HOST)


def _duckdb_failure_text(nb) -> str:
    """Error text of whichever cell failed, for transient classification.

    A failing ``%%bash`` cell raises ``CalledProcessError`` whose message only
    embeds the cell *source* (always contains the CDN URL) — the real error
    ("could not resolve host", an HTTP 5xx) lands on the cell's stderr *stream*.
    So classify from the failing cell's captured outputs, not from ``exc``.
    """
    parts: list[str] = []
    for cell in nb.cells:
        outputs = cell.get("outputs", [])
        if not any(o.get("output_type") == "error" for o in outputs):
            continue
        for out in outputs:
            if out.get("output_type") == "stream":
                parts.append(out.get("text", ""))
            elif out.get("output_type") == "error" and out.get("ename") != "CalledProcessError":
                # A %%bash failure's CalledProcessError embeds the cell SOURCE
                # (which carries the remote URLs), so a deterministic error like
                # "timed out" would spuriously satisfy the host+signature gate.
                # Classify %%bash cells from their stderr stream only; a real
                # Python exception carries the actual error in evalue/traceback.
                parts.append(out.get("evalue", ""))
                parts.append("\n".join(out.get("traceback", [])))
    return "\n".join(parts)


def _duckdb_network_error_is_transient(error_text: str) -> bool:
    text = (error_text or "").lower()
    if not any(host in text for host in _DUCKDB_REMOTE_HOSTS):
        return False
    return any(re.search(pattern=pattern, string=text) for pattern in _DUCKDB_TRANSIENT_SIGNATURES)


def test_notebook_runs_without_errors(notebook_path, request):
    """Execute the notebook and assert it completes without errors."""
    rel = str(notebook_path.relative_to(EXAMPLES_DIR))
    if rel in _KNOWN_FAILING_NOTEBOOKS:
        request.applymarker(pytest.mark.xfail(
            reason=_KNOWN_FAILING_NOTEBOOKS[rel],
            strict=False,
        ))
    is_metricflow = _METRICFLOW_NB_DIR in notebook_path.parts
    if is_metricflow:
        complete_marker = notebook_path.parent / ".cache" / ".complete"
        if not complete_marker.exists() and not _github_reachable():
            pytest.skip("GitHub unreachable; cannot bootstrap the MetricFlow notebook")

    is_duckdb = _DUCKDB_NB_DIR in notebook_path.parts
    if is_duckdb and not _duckdb_data_host_reachable():
        pytest.skip("jsDelivr CDN unreachable; cannot run the DuckDB httpfs notebook")

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
    except nbclient.exceptions.CellTimeoutError as exc:
        # The DuckDB notebooks normally finish in ~15s; a 600s timeout means a
        # network hang (stalled CDN, installer, or extension download), not a
        # code bug. Other notebooks still fail loudly on timeout.
        if is_duckdb:
            pytest.skip(f"DuckDB notebook timed out (likely a network hang): {exc}")
        raise
    except nbclient.exceptions.CellExecutionError as exc:
        # A failing `git fetch` (or a stale/partial cache) surfaces as
        # MetricFlowDemoError mid-run. Skip — rather than report a bootstrap
        # failure as a notebook bug — when GitHub is unreachable *or* the failure
        # is a transient network/server hiccup (e.g. a 503 over a reachable
        # socket). A deterministic MetricFlowDemoError (bad pin, missing CSVs)
        # still fails loudly.
        if is_metricflow and "MetricFlowDemoError" in str(exc):
            text = str(exc)
            if not _github_reachable() or _bootstrap_failure_is_transient(text):
                pytest.skip(f"MetricFlow notebook could not bootstrap: {exc}")
        # Narrow by design: classify the failing cell's captured stderr/error
        # output, never ``str(exc)`` or a ``%%bash`` ``evalue`` — both embed the
        # cell source, which always contains the CDN URL, so the host-name gate
        # would always pass and a real NotImplementedError / assertion / query
        # bug could be misread as transient. The pre-run probe already covers
        # "network down at start".
        if is_duckdb and _duckdb_network_error_is_transient(_duckdb_failure_text(nb)):
            pytest.skip(f"DuckDB notebook hit a transient network error: {exc}")
        raise
