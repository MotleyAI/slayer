"""Setup helper for the dbt MetricFlow -> SLayer demo notebook.

Self-bootstrapping: clones the dbt-labs ACME Insurance benchmark project at a
**pinned commit**, loads its CSV data into a local DuckDB file, and converts the
dbt MetricFlow definitions (``semantic_models`` + ``metrics``) into SLayer models
via :class:`~slayer.dbt.converter.DbtToSlayerConverter`. Everything generated
lives under a gitignored ``.cache/`` next to this file, so nothing generated is
committed and re-runs are instant.

The dbt project is pinned to an exact commit (``DBT_PIN_SHA``) rather than a
branch tip: the upstream branch is mutable, and the notebook asserts hard-coded
gold numbers, so a moving branch could silently invalidate them. Fetching the
pinned SHA keeps the demo reproducible regardless of upstream movement.

Gold-query helper note: SLayer opens the DuckDB file through SQLAlchemy with a
read-write engine that DuckDB will not let a second raw connection share under a
different configuration. So :func:`fetch_gold` must be called **before** any
SLayer query touches the file — the notebook precomputes all gold answers up
front for exactly this reason.

Returns from :func:`ensure_metricflow_demo`: ``(client, db_path, result)``.
"""

import logging
import shutil
import subprocess
from pathlib import Path
from typing import List

import duckdb

from slayer.async_utils import run_sync
from slayer.client.slayer_client import SlayerClient
from slayer.core.models import DatasourceConfig
from slayer.dbt.converter import ConversionResult, DbtToSlayerConverter
from slayer.dbt.parser import parse_dbt_project
from slayer.storage.yaml_storage import YAMLStorage

logger = logging.getLogger(__name__)

DBT_REPO_URL = "https://github.com/dbt-labs/semantic-layer-llm-benchmarking.git"
# Pinned commit on the ``refresh-2025-additional-models`` branch (the branch that
# ships the 3 bridge models + the derived/filtered MetricFlow metrics this demo
# showcases). Update this SHA *and* the notebook's gold numbers together if the
# upstream data ever changes.
DBT_PIN_SHA = "e4bdee5baeaa9b0ecb8345315c4adfffbeb2f0d1"
DATASOURCE_NAME = "acme_duckdb"

_THIS_DIR = Path(__file__).resolve().parent
CACHE_DIR = _THIS_DIR / ".cache"
DBT_CHECKOUT = CACHE_DIR / "semantic-layer-llm-benchmarking"
DB_PATH = CACHE_DIR / "acme.duckdb"
MODELS_DIR = CACHE_DIR / "slayer_models"
_COMPLETE_MARKER = CACHE_DIR / ".complete"
_CSV_SUBDIR = "ACME_Insurance/data"


class MetricFlowDemoError(RuntimeError):
    """Raised when the demo cannot bootstrap (e.g. clone failed). Recognizable
    so the integration-test skip guard can distinguish a network failure from a
    genuine conversion bug."""


def _git(*args: str, cwd: Path) -> str:
    """Run a git command, returning stripped stdout. Raises on failure."""
    result = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def _checkout_is_valid(checkout: Path) -> bool:
    """True iff ``checkout`` is a git repo whose HEAD is the pinned commit."""
    if not (checkout / ".git").exists():
        return False
    try:
        return _git("rev-parse", "HEAD", cwd=checkout) == DBT_PIN_SHA
    except (subprocess.CalledProcessError, OSError):
        # OSError covers a missing `git` binary; treat as "not valid" so the
        # caller falls through to the clone path, which raises the recognized
        # MetricFlowDemoError instead of a raw FileNotFoundError.
        return False


def clone_dbt_project() -> Path:
    """Fetch the dbt project at the pinned commit into ``DBT_CHECKOUT``.

    Idempotent: if a valid checkout (HEAD == pinned SHA) already exists, it is
    reused. Otherwise the project is fetched into a temp dir and atomically
    renamed into place, so a partial/failed clone never leaves a directory that
    later looks like a usable cache.
    """
    if _checkout_is_valid(DBT_CHECKOUT):
        logger.info("Reusing cached dbt checkout at %s", DBT_CHECKOUT)
        return DBT_CHECKOUT

    # Drop any stale/partial checkout before re-fetching.
    if DBT_CHECKOUT.exists():
        shutil.rmtree(DBT_CHECKOUT)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_DIR / "_clone_tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)

    try:
        # Shallow-fetch the exact pinned commit. GitHub allows fetching an
        # unadvertised SHA, so we never depend on the branch tip.
        _git("init", "-q", cwd=tmp)
        _git("remote", "add", "origin", DBT_REPO_URL, cwd=tmp)
        _git("fetch", "--depth", "1", "origin", DBT_PIN_SHA, cwd=tmp)
        _git("checkout", "-q", "FETCH_HEAD", cwd=tmp)
        head = _git("rev-parse", "HEAD", cwd=tmp)
        if head != DBT_PIN_SHA:
            raise MetricFlowDemoError(
                f"Fetched commit {head} != pinned {DBT_PIN_SHA}; update the pin."
            )
    except (subprocess.CalledProcessError, OSError) as exc:
        # OSError covers a missing `git` binary (FileNotFoundError); both map to
        # the structured demo error the notebook / test skip-guard recognise.
        shutil.rmtree(tmp, ignore_errors=True)
        detail = getattr(exc, "stderr", None) or exc
        raise MetricFlowDemoError(
            f"Failed to clone {DBT_REPO_URL} @ {DBT_PIN_SHA}: {detail}"
        ) from exc

    tmp.rename(DBT_CHECKOUT)
    logger.info("Cloned dbt project @ %s into %s", DBT_PIN_SHA, DBT_CHECKOUT)
    return DBT_CHECKOUT


def load_csvs_into_duckdb(csv_dir: Path, db_path: Path) -> List[str]:
    """Load every ACME Insurance CSV into its own DuckDB table.

    Table name = CSV filename stem (e.g. ``Claim.csv`` -> ``Claim``). Overwrites
    any existing database file. Returns the created table names.
    """
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))
    try:
        csv_files = sorted(csv_dir.glob("*.csv"))
        if not csv_files:
            raise MetricFlowDemoError(f"No CSV files found in {csv_dir}")
        for csv_file in csv_files:
            # Bind the path as a parameter so an apostrophe in the checkout path
            # can't break the SQL string.
            conn.execute(
                f'CREATE TABLE "{csv_file.stem}" AS '
                "SELECT * FROM read_csv_auto(?, header=true)",
                [str(csv_file)],
            )
        # FireClaim.Premium is all-NULL in the CSV, so DuckDB infers VARCHAR;
        # coerce to DOUBLE so the numeric measure converts/queries correctly.
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
        if "FireClaim" in tables:
            conn.execute("ALTER TABLE FireClaim ALTER Premium TYPE DOUBLE")
        return sorted(tables)
    finally:
        conn.close()


def convert_dbt_to_slayer(
    dbt_project_path: Path, models_dir: Path, db_path: Path
) -> ConversionResult:
    """Convert the dbt MetricFlow project into SLayer models and persist them.

    Parses ``<dbt_project_path>/models`` for ``semantic_models`` + ``metrics``,
    runs :class:`DbtToSlayerConverter`, and saves each model plus a DuckDB
    datasource config into a fresh ``YAMLStorage`` rooted at ``models_dir``.
    """
    # Quieten the converter's benign "foreign entity '…' has no matching primary
    # entity" notices so the notebook output stays focused on the demo.
    logging.getLogger("slayer.dbt").setLevel(logging.ERROR)

    if models_dir.exists():
        shutil.rmtree(models_dir)

    project = parse_dbt_project(str(dbt_project_path / "models"))
    result = DbtToSlayerConverter(
        project=project, data_source=DATASOURCE_NAME
    ).convert()

    storage = YAMLStorage(base_dir=str(models_dir))
    for model in result.models:
        run_sync(storage.save_model(model))
    run_sync(
        storage.save_datasource(
            DatasourceConfig(
                name=DATASOURCE_NAME,
                type="duckdb",
                database=str(db_path.resolve()),
            )
        )
    )
    return result


def fetch_gold(db_path: Path, sql: str) -> List[dict]:
    """Run a raw gold SQL query against the DuckDB file and return rows as dicts.

    MUST be called before any SLayer query opens ``db_path``: SLayer holds a
    read-write engine on the file that a second raw connection cannot share, so
    the notebook precomputes all gold answers up front.
    """
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        cur = conn.execute(sql)
        columns = [c[0] for c in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def ensure_dbt_data() -> Path:
    """Clone the pinned dbt project and load its CSVs into DuckDB; return the
    dbt checkout path.

    Idempotent: a completed prior run (valid checkout + completeness marker +
    DuckDB file) is reused without touching the network. A partial prior run is
    rebuilt from scratch. The dbt -> SLayer conversion is a **separate**,
    always-run step (:func:`convert_dbt_to_slayer`) so the notebook can show it
    as its own explicit cell.
    """
    reuse = (
        _COMPLETE_MARKER.exists()
        and _checkout_is_valid(DBT_CHECKOUT)
        and DB_PATH.exists()
    )
    if reuse:
        logger.info("Reusing cached dbt checkout + DuckDB under %s", CACHE_DIR)
        return DBT_CHECKOUT

    if _COMPLETE_MARKER.exists():
        _COMPLETE_MARKER.unlink()
    dbt_path = clone_dbt_project()
    csv_dir = dbt_path / _CSV_SUBDIR
    if not csv_dir.exists():
        raise MetricFlowDemoError(f"CSV data dir not found: {csv_dir}")
    load_csvs_into_duckdb(csv_dir=csv_dir, db_path=DB_PATH)
    _COMPLETE_MARKER.touch()
    return dbt_path


def ensure_metricflow_demo() -> "tuple[SlayerClient, Path, ConversionResult]":
    """One-shot convenience: ensure data, convert, and build a ready client.

    Returns ``(client, db_path, result)``. Notebooks that want to show the
    conversion as an explicit step call :func:`ensure_dbt_data` and
    :func:`convert_dbt_to_slayer` separately instead.
    """
    dbt_path = ensure_dbt_data()
    result = convert_dbt_to_slayer(
        dbt_project_path=dbt_path, models_dir=MODELS_DIR, db_path=DB_PATH
    )
    client = SlayerClient(storage=YAMLStorage(base_dir=str(MODELS_DIR)))
    return client, DB_PATH, result
