"""Setup helper for the Cube -> SLayer demo notebook.

Self-contained and **fully offline**: builds a tiny jaffle-shop-flavored DuckDB
with deterministic rows, and imports the committed ``cube_project`` (YAML cubes +
a view + a JS ``FILTER_PARAMS`` cube) into SLayer models. Cube import needs no
database connection — types come from the Cube declarations — so the DuckDB is
only used to *query* the imported models.

Two import paths are exposed: :func:`import_cube_cli` runs the real
``slayer import-cube`` command (what the notebook shows), and
:func:`import_cube_lib` runs the same conversion in-process (used by the test).
Both wipe the model directory first, so the datasource must be saved *after*
importing.

Gold-answer helper note: SLayer opens the DuckDB file through a read-write engine
that a second raw connection cannot share, so :func:`compute_gold` must run
*before* any SLayer query touches the file — the notebook precomputes all gold
answers up front for exactly this reason.
"""

import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Union

import duckdb

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.parser import parse_cube_project
from slayer.cube.report import CubeConversionResult
from slayer.storage.yaml_storage import YAMLStorage

logger = logging.getLogger(__name__)

DATASOURCE_NAME = "shop_cube"

_THIS_DIR = Path(__file__).resolve().parent
CUBE_PROJECT = _THIS_DIR / "cube_project"
CACHE_DIR = _THIS_DIR / ".cache"
DB_PATH = CACHE_DIR / "shop.duckdb"
MODELS_DIR = CACHE_DIR / "slayer_models"
REPORT_PATH = CACHE_DIR / "cube_import_report.json"

_PathLike = Union[str, Path]

# DuckDB DDL for the three tables the cubes bind to.
_SCHEMA = [
    "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, name VARCHAR, region VARCHAR)",
    "CREATE TABLE products (product_id INTEGER PRIMARY KEY, name VARCHAR, category VARCHAR)",
    "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, "
    "product_id INTEGER, amount DOUBLE, quantity INTEGER, ordered_at DATE, status VARCHAR)",
]

# Deterministic rows so every gold number below is exact.
_CUSTOMERS = [
    (1, "Alice", "North"),
    (2, "Bob", "North"),
    (3, "Carol", "South"),
]
_PRODUCTS = [
    (1, "Latte", "Beverages"),
    (2, "Bagel", "Bakery"),
]
_ORDERS = [
    # order_id, customer_id, product_id, amount, quantity, ordered_at, status
    (1, 1, 1, 100.0, 2, "2024-01-01", "completed"),
    (2, 1, 2, 200.0, 1, "2024-01-05", "completed"),
    (3, 2, 1, 300.0, 3, "2024-02-01", "completed"),
    (4, 2, 2, 150.0, 1, "2024-02-10", "pending"),
    (5, 3, 1, 250.0, 5, "2024-03-01", "completed"),
    (6, 3, 2, 400.0, 2, "2024-03-15", "completed"),
]


def build_shop_duckdb(db_path: _PathLike = DB_PATH) -> Path:
    """Create the retail DuckDB (three tables + deterministic rows).

    Overwrites any existing file so a re-run always starts from a clean, known
    dataset. Returns the database path.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))
    try:
        for ddl in _SCHEMA:
            conn.execute(ddl)
        conn.executemany("INSERT INTO customers VALUES (?, ?, ?)", _CUSTOMERS)
        conn.executemany("INSERT INTO products VALUES (?, ?, ?)", _PRODUCTS)
        conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)", _ORDERS)
    finally:
        conn.close()
    logger.info("Built retail DuckDB at %s", db_path)
    return db_path


def import_cube_lib(
    cube_project: _PathLike = CUBE_PROJECT,
    models_dir: _PathLike = MODELS_DIR,
) -> CubeConversionResult:
    """Import the cube_project in-process and persist the models.

    Wipes ``models_dir`` first (so the datasource must be saved afterwards),
    parses the project, runs :class:`CubeToSlayerConverter`, and saves each
    model. Returns the full :class:`CubeConversionResult` (models + report).
    """
    models_dir = Path(models_dir)
    if models_dir.exists():
        shutil.rmtree(models_dir)

    project, parse_issues = parse_cube_project(str(Path(cube_project).resolve()))
    result = CubeToSlayerConverter(
        project=project, data_source=DATASOURCE_NAME, parse_issues=parse_issues
    ).convert()

    storage = YAMLStorage(base_dir=str(models_dir))
    for model in result.models:
        run_sync(storage.save_model(model))
    return result


def import_cube_cli(
    cube_project: _PathLike = CUBE_PROJECT,
    models_dir: _PathLike = MODELS_DIR,
    report_path: _PathLike = REPORT_PATH,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """Run the real ``slayer import-cube`` CLI as a subprocess.

    Uses the current interpreter (``python -m slayer.cli``) and absolute paths so
    it behaves identically from a notebook or a test. Wipes ``models_dir`` first.
    With ``check`` (the default) a non-zero exit raises with both streams in the
    message; the test passes ``check=True`` and inspects the returned process.
    """
    models_dir = Path(models_dir)
    report_path = Path(report_path)
    if models_dir.exists():
        shutil.rmtree(models_dir)
    models_dir.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", "slayer.cli", "import-cube",
        str(Path(cube_project).resolve()),
        "--datasource", DATASOURCE_NAME,
        "--storage", str(models_dir.resolve()),
        "--report", str(report_path.resolve()),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(_THIS_DIR))
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"`slayer import-cube` failed (exit {proc.returncode}).\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc


def save_datasource(
    models_dir: _PathLike = MODELS_DIR, db_path: _PathLike = DB_PATH
) -> None:
    """Register the DuckDB as a SLayer datasource in the model store.

    Cube import files models under the datasource *name* but never creates the
    datasource itself, so querying the imported models needs this. Call it AFTER
    importing (the importers wipe ``models_dir``).
    """
    storage = YAMLStorage(base_dir=str(Path(models_dir)))
    ds = DatasourceConfig(
        name=DATASOURCE_NAME, type="duckdb", database=str(Path(db_path).resolve())
    )
    run_sync(storage.save_datasource(ds))


def fetch_gold(db_path: _PathLike, sql: str) -> List[dict]:
    """Run a raw gold SQL query against the DuckDB file and return rows as dicts.

    MUST be called before any SLayer query opens ``db_path``: SLayer holds a
    read-write engine on the file that a second raw connection cannot share.
    """
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        cur = conn.execute(sql)
        columns = [c[0] for c in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


_GOLD_BY_REGION_SQL = """
    SELECT c.region AS region, SUM(o.amount) AS amount
    FROM orders o JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY c.region
    ORDER BY c.region
"""


def compute_gold(db_path: _PathLike = DB_PATH) -> dict:
    """Compute all reference answers up front (before SLayer opens ``db_path``).

    Keys: ``total`` (all order amount), ``by_region`` (list of ``{region,
    amount}``), and the ``order_facts`` counts ``of_all`` / ``of_north`` /
    ``of_south`` / ``of_beverages``.
    """
    one = lambda sql: fetch_gold(db_path, sql)[0]["v"]  # noqa: E731
    return {
        "total": one("SELECT SUM(amount) AS v FROM orders"),
        "by_region": fetch_gold(db_path, _GOLD_BY_REGION_SQL),
        "of_all": one("SELECT COUNT(*) AS v FROM orders"),
        "of_north": one(
            "SELECT COUNT(*) AS v FROM orders o "
            "JOIN customers c ON o.customer_id = c.customer_id WHERE c.region = 'North'"
        ),
        "of_south": one(
            "SELECT COUNT(*) AS v FROM orders o "
            "JOIN customers c ON o.customer_id = c.customer_id WHERE c.region = 'South'"
        ),
        "of_beverages": one(
            "SELECT COUNT(*) AS v FROM orders o "
            "JOIN products p ON o.product_id = p.product_id WHERE p.category = 'Beverages'"
        ),
    }
