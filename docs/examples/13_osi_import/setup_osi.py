"""Setup helper for the OSI -> SLayer demo notebook.

Self-contained and **fully offline** (no network, unlike the dbt MetricFlow
demo's git clone): builds a tiny retail DuckDB with deterministic rows, registers
it as a SLayer datasource, and converts the committed ``shop.osi.yaml`` OSI
config into queryable SLayer models via
:class:`~slayer.osi.converter.OsiToSlayerConverter`.

Everything generated lives under a gitignored ``.cache/`` next to this file, so
nothing generated is committed and re-runs rebuild it cheaply.

The OSI importer takes column *types* from **live introspection** of the
datasource (OSI carries no type hints), so the datasource must be saved and
reachable *before* :func:`convert_osi_to_slayer` runs.

Gold-query helper note: SLayer opens the DuckDB file through SQLAlchemy with a
read-write engine that DuckDB will not let a second raw connection share under a
different configuration. So :func:`fetch_gold` must be called **before** any
SLayer query touches the file — the notebook precomputes all gold answers up
front for exactly this reason.

Returns from :func:`ensure_osi_demo`: ``(client, db_path, result)``.
"""

import logging
import shutil
from pathlib import Path
from typing import List

import duckdb

from slayer.async_utils import run_sync
from slayer.client.slayer_client import SlayerClient
from slayer.core.models import DatasourceConfig
from slayer.ingest_report import ConversionResult
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.parser import parse_osi_path
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage

logger = logging.getLogger(__name__)

DATASOURCE_NAME = "shop_osi"

_THIS_DIR = Path(__file__).resolve().parent
OSI_CONFIG = _THIS_DIR / "shop.osi.yaml"
CACHE_DIR = _THIS_DIR / ".cache"
DB_PATH = CACHE_DIR / "shop.duckdb"
MODELS_DIR = CACHE_DIR / "slayer_models"

# DuckDB DDL for the four tables the OSI config binds to. Mirrors the schema in
# tests/test_cli_import_osi.py, with DuckDB-native types (DOUBLE / VARCHAR).
_SCHEMA = [
    "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, "
    "product_id INTEGER, amount DOUBLE, quantity INTEGER, ordered_at DATE, status VARCHAR)",
    "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, region_id INTEGER, "
    "name VARCHAR, segment VARCHAR)",
    "CREATE TABLE products (product_id INTEGER PRIMARY KEY, category VARCHAR, price DOUBLE)",
    "CREATE TABLE regions (region_id INTEGER PRIMARY KEY, name VARCHAR, population INTEGER)",
]

# Deterministic rows so every gold number below is exact.
_REGIONS = [
    (1, "North", 1000),
    (2, "South", 2000),
]
_CUSTOMERS = [
    (1, 1, "Alice", "consumer"),
    (2, 1, "Bob", "business"),
    (3, 2, "Carol", "consumer"),
]
_PRODUCTS = [
    (1, "Beverages", 5.0),
    (2, "Bakery", 3.0),
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


def build_shop_duckdb(db_path: Path = DB_PATH) -> Path:
    """Create the retail DuckDB (four tables + deterministic rows).

    Overwrites any existing file so a re-run always starts from a clean, known
    dataset. Returns the database path.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))
    try:
        for ddl in _SCHEMA:
            conn.execute(ddl)
        conn.executemany("INSERT INTO regions VALUES (?, ?, ?)", _REGIONS)
        conn.executemany("INSERT INTO customers VALUES (?, ?, ?, ?)", _CUSTOMERS)
        conn.executemany("INSERT INTO products VALUES (?, ?, ?)", _PRODUCTS)
        conn.executemany(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)", _ORDERS
        )
    finally:
        conn.close()
    logger.info("Built retail DuckDB at %s", db_path)
    return db_path


def convert_osi_to_slayer(
    osi_path: Path = OSI_CONFIG,
    models_dir: Path = MODELS_DIR,
    db_path: Path = DB_PATH,
) -> ConversionResult:
    """Convert the OSI config into SLayer models and persist them.

    Saves a DuckDB ``DatasourceConfig`` into a fresh ``YAMLStorage`` rooted at
    ``models_dir`` (the importer introspects it live for column types), parses
    the OSI documents, runs :class:`OsiToSlayerConverter`, and saves each model.
    """
    # Quieten the converter's benign clean-fail notices so the notebook output
    # stays focused on the demo.
    logging.getLogger("slayer.osi").setLevel(logging.ERROR)

    if models_dir.exists():
        shutil.rmtree(models_dir)

    storage = YAMLStorage(base_dir=str(models_dir))
    ds = DatasourceConfig(
        name=DATASOURCE_NAME, type="duckdb", database=str(db_path.resolve())
    )
    run_sync(storage.save_datasource(ds))

    documents = parse_osi_path(osi_path)
    sa_engine = engine_factory.get_engine(ds.resolve_env_vars())
    result = OsiToSlayerConverter(
        documents=documents,
        data_source=DATASOURCE_NAME,
        sa_engine=sa_engine,
        dialect="ANSI_SQL",
        target_dialect="duckdb",
    ).convert()

    for model in result.models:
        run_sync(storage.save_model(model))
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


# --- Reference ("gold") answers, shared by both demo notebooks ---------------
# Hand-written SQL whose results are the trusted numbers every SLayer query in
# the notebooks is checked against. `rev_plus_pop` mirrors SLayer's sub-query
# isolation: the joined `regions.population` is summed at the *regions* grain
# (distinct regions the orders reach), not once per order.

_GOLD_BY_REGION_SQL = """
    SELECT r.name AS region, SUM(o.amount) AS amount
    FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN regions r ON c.region_id = r.region_id
    GROUP BY r.name
    ORDER BY r.name
"""

_GOLD_REV_PLUS_POP_SQL = """
    WITH reached AS (
        SELECT DISTINCT c.region_id
        FROM orders o JOIN customers c ON o.customer_id = c.customer_id
    )
    SELECT (SELECT SUM(amount) FROM orders)
         + (SELECT SUM(population) FROM regions
            WHERE region_id IN (SELECT region_id FROM reached)) AS value
"""


def compute_gold(db_path: Path = DB_PATH) -> dict:
    """Compute all reference answers up front (before SLayer opens ``db_path``).

    Returns a dict with keys ``total``, ``by_region``, ``aov``, ``cust_reach``,
    ``rev_plus_pop`` — the expected values both notebooks assert their SLayer /
    MCP query results against.
    """
    return {
        "total": fetch_gold(db_path, "SELECT SUM(amount) AS v FROM orders")[0]["v"],
        "by_region": fetch_gold(db_path, _GOLD_BY_REGION_SQL),
        "aov": fetch_gold(
            db_path, "SELECT SUM(amount) * 1.0 / COUNT(*) AS v FROM orders"
        )[0]["v"],
        "cust_reach": fetch_gold(
            db_path,
            "SELECT SUM(amount) * 1.0 / COUNT(DISTINCT customer_id) AS v FROM orders",
        )[0]["v"],
        "rev_plus_pop": fetch_gold(db_path, _GOLD_REV_PLUS_POP_SQL)[0]["value"],
    }


def ensure_osi_demo() -> "tuple[SlayerClient, Path, ConversionResult]":
    """One-shot convenience: build data, convert OSI, and return a ready client.

    Returns ``(client, db_path, result)``. Notebooks that want to show the build
    and conversion as explicit steps call :func:`build_shop_duckdb` and
    :func:`convert_osi_to_slayer` separately instead.
    """
    build_shop_duckdb(DB_PATH)
    result = convert_osi_to_slayer(OSI_CONFIG, MODELS_DIR, DB_PATH)
    client = SlayerClient(storage=YAMLStorage(base_dir=str(MODELS_DIR)))
    return client, DB_PATH, result
