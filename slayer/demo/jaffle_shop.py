"""Jaffle Shop demo dataset — bundled, one-command setup.

Generates ~1 year of synthetic coffee-shop data via ``jafgen``, loads it into
a DuckDB file under the storage directory, registers a ``jaffle_shop``
datasource, and (optionally) auto-ingests SLayer models.

All operations are idempotent: re-running with the same storage path reuses
the existing DuckDB file instead of regenerating.

``jafgen`` is a git-only install (not on PyPI) — missing-dependency errors
surface with a ready-to-copy install command rather than an ImportError.
"""

import datetime as dt
import os
import shutil
import subprocess
import tempfile
from typing import TYPE_CHECKING, List, Optional, Tuple

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.storage.base import StorageBackend, storage_base_dir

if TYPE_CHECKING:
    import duckdb

DEMO_NAME = "jaffle_shop"

DEFAULT_TIME_DIMENSIONS = {
    "orders": "ordered_at",
    "tweets": "tweeted_at",
}

# Monetary columns stored in cents in the jafgen CSVs; converted to dollars on load.
CENTS_COLUMNS = {
    "orders": ["subtotal", "tax_paid", "order_total"],
    "products": ["price"],
    "supplies": ["cost"],
}

LOAD_ORDER = ["customers", "stores", "products", "orders", "order_items", "supplies", "tweets"]

# CSV file stem (jafgen's ``raw_<name>.csv``) → DuckDB table name. Names match
# 1:1 today, but the mapping keeps the seam open if jafgen renames a file.
TABLE_NAMES = {name: name for name in LOAD_ORDER}

# Date columns per table — used to shift jafgen's hard-coded 2018-09-01 epoch
# forward so the demo data always ends near "today".
DATE_COLUMNS = {
    "stores": ["opened_at"],
    "orders": ["ordered_at"],
    "tweets": ["tweeted_at"],
}

JAFGEN_GIT_URL = (
    "git+https://github.com/rossbowen/jaffle-shop-generator.git"
    "@09557a1118b000071f8171aa97d54d5029bf0f0b"
)

JAFFLE_SCHEMA_SQL = """\
CREATE TABLE customers (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE stores (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    opened_at DATE NOT NULL,
    tax_rate DOUBLE NOT NULL
);

CREATE TABLE products (
    sku VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    price DOUBLE NOT NULL,
    description VARCHAR NOT NULL
);

CREATE TABLE orders (
    id VARCHAR PRIMARY KEY,
    customer_id VARCHAR NOT NULL REFERENCES customers(id),
    ordered_at DATE NOT NULL,
    store_id VARCHAR NOT NULL REFERENCES stores(id),
    subtotal DOUBLE NOT NULL,
    tax_paid DOUBLE NOT NULL,
    order_total DOUBLE NOT NULL
);

CREATE TABLE order_items (
    id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL REFERENCES orders(id),
    sku VARCHAR NOT NULL REFERENCES products(sku),
    quantity INTEGER NOT NULL
);

CREATE TABLE supplies (
    id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    cost DOUBLE NOT NULL,
    perishable VARCHAR NOT NULL,
    sku VARCHAR NOT NULL REFERENCES products(sku),
    PRIMARY KEY (id, sku)
);

CREATE TABLE tweets (
    id VARCHAR PRIMARY KEY,
    user_id VARCHAR NOT NULL REFERENCES customers(id),
    tweeted_at DATE NOT NULL,
    content VARCHAR NOT NULL
);
"""


class DemoDependencyError(RuntimeError):
    """Raised when ``duckdb`` or ``jafgen`` is missing.

    The message is pre-formatted with install hints; callers should print it
    as-is and exit with a non-zero status.
    """


def _missing_deps_message(missing: List[str]) -> str:
    lines = ["The Jaffle Shop demo requires additional dependencies:"]
    if "duckdb" in missing:
        lines.append("  - duckdb: pip install duckdb")
    if "jafgen" in missing:
        lines.append(f"  - jafgen:  pip install '{JAFGEN_GIT_URL}'")
    lines.append("")
    lines.append("Install the missing packages and re-run the command.")
    return "\n".join(lines)


def check_dependencies() -> None:
    """Verify that ``duckdb`` and ``jafgen`` are available.

    Raises ``DemoDependencyError`` with a ready-to-print install hint if not.
    """
    missing: List[str] = []
    try:
        import duckdb  # noqa: F401
    except ImportError:
        missing.append("duckdb")
    if shutil.which("jafgen") is None:
        missing.append("jafgen")
    if missing:
        raise DemoDependencyError(_missing_deps_message(missing))


def resolve_demo_db_path(storage_path: str) -> str:
    """Return the path to the Jaffle Shop DuckDB file for a given storage path.

    Creates the parent ``demo/`` directory if missing.
    """
    demo_dir = os.path.join(storage_base_dir(storage_path), "demo")
    os.makedirs(demo_dir, exist_ok=True)
    return os.path.join(demo_dir, "jaffle_shop.duckdb")


def generate_data(output_dir: str, years: int = 1, days: int = 0) -> str:
    """Run ``jafgen`` into ``output_dir``; return the path to the generated CSVs."""
    cmd = ["jafgen", str(max(1, years))]
    if days > 0:
        cmd.extend(["--days", str(days)])
    try:
        subprocess.run(args=cmd, cwd=output_dir, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode("utf-8", errors="replace") if e.stderr else ""
        raise RuntimeError(f"jafgen failed: {stderr.strip() or e}") from e
    return os.path.join(output_dir, "jaffle-data")


def create_schema(
    conn: "duckdb.DuckDBPyConnection",
    schema_path: Optional[str] = None,
) -> None:
    """Create the Jaffle Shop tables on ``conn``.

    By default uses the schema bundled in ``JAFFLE_SCHEMA_SQL``; pass
    ``schema_path`` to load a custom ``.sql`` file instead.
    """
    if schema_path is None:
        sql = JAFFLE_SCHEMA_SQL
    else:
        with open(schema_path) as f:
            sql = f.read()
    # Strip SQL comments (lines starting with ``--``) before splitting on ``;``.
    lines = [line for line in sql.splitlines() if not line.strip().startswith("--")]
    clean_sql = "\n".join(lines)
    for statement in clean_sql.split(";"):
        statement = statement.strip()
        if statement:
            conn.execute(statement)


def shift_dates_to_today(conn: "duckdb.DuckDBPyConnection") -> None:
    """Shift all date columns forward so ``MAX(orders.ordered_at) == today``.

    jafgen hard-codes its simulation epoch at 2018-09-01, so the generated
    data always ends on a fixed historical date regardless of the ``years``
    argument. This post-load step keeps the demo feeling current without
    having to patch jafgen internals.
    """
    row = conn.execute("SELECT MAX(ordered_at) FROM orders").fetchone()
    if row is None or row[0] is None:
        return
    max_date: dt.date = row[0]
    delta_days = (dt.date.today() - max_date).days
    if delta_days == 0:
        return
    for table, cols in DATE_COLUMNS.items():
        for col in cols:
            conn.execute(
                f"UPDATE {table} SET {col} = {col} + INTERVAL '{delta_days} days'"
            )


def load_data(
    conn: "duckdb.DuckDBPyConnection",
    data_dir: str,
    prefix: str = "raw",
) -> None:
    """Load jafgen CSVs from ``data_dir`` into the already-created tables.

    Monetary columns listed in ``CENTS_COLUMNS`` are converted from cents to
    dollars on insert. After loading, all ``DATE_COLUMNS`` are shifted forward
    so the latest ``orders.ordered_at`` is today (see ``shift_dates_to_today``).
    """
    for csv_name in LOAD_ORDER:
        table = TABLE_NAMES[csv_name]
        csv_path = os.path.join(data_dir, f"{prefix}_{csv_name}.csv")
        if not os.path.exists(csv_path):
            continue

        # read_csv_auto takes a SQL string literal, not a bound parameter, so
        # escape any single quotes in the path by doubling them before
        # interpolation. (The path itself is trusted in normal demo flows —
        # this is defence-in-depth for callers who pass arbitrary data_dirs.)
        quoted_csv_path = csv_path.replace("'", "''")

        cents_cols = CENTS_COLUMNS.get(csv_name, [])
        if cents_cols:
            conn.execute(
                f"CREATE TEMP VIEW _{table}_raw AS SELECT * FROM read_csv_auto('{quoted_csv_path}')"
            )
            columns = [row[0] for row in conn.execute(f"DESCRIBE _{table}_raw").fetchall()]
            select_parts = []
            for col in columns:
                if col in cents_cols:
                    select_parts.append(f"CAST({col} AS DOUBLE) / 100.0 AS {col}")
                else:
                    select_parts.append(col)
            conn.execute(
                f"INSERT INTO {table} SELECT {', '.join(select_parts)} FROM _{table}_raw"
            )
            conn.execute(f"DROP VIEW _{table}_raw")
        else:
            conn.execute(
                f"INSERT INTO {table} SELECT * FROM read_csv_auto('{quoted_csv_path}')"
            )

    shift_dates_to_today(conn)


def verify(conn: "duckdb.DuckDBPyConnection") -> dict:
    """Return row counts, FK-orphan counts, and a couple of sample joins.

    Useful as a smoke test after a fresh load.
    """
    results: dict = {}

    counts = {}
    for table in TABLE_NAMES.values():
        counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    results["row_counts"] = counts

    fk_checks = {
        "orders->customers": "SELECT COUNT(*) FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)",
        "orders->stores": "SELECT COUNT(*) FROM orders WHERE store_id NOT IN (SELECT id FROM stores)",
        "order_items->orders": "SELECT COUNT(*) FROM order_items WHERE order_id NOT IN (SELECT id FROM orders)",
        "order_items->products": "SELECT COUNT(*) FROM order_items WHERE sku NOT IN (SELECT sku FROM products)",
        "supplies->products": "SELECT COUNT(*) FROM supplies WHERE sku NOT IN (SELECT sku FROM products)",
        "tweets->customers": "SELECT COUNT(*) FROM tweets WHERE user_id NOT IN (SELECT id FROM customers)",
    }
    results["fk_orphans"] = {
        name: conn.execute(query).fetchone()[0] for name, query in fk_checks.items()
    }

    results["revenue_by_store"] = conn.execute(
        """
        SELECT s.name, COUNT(*) AS order_count, SUM(o.order_total) AS revenue
        FROM orders o JOIN stores s ON o.store_id = s.id
        GROUP BY s.name ORDER BY revenue DESC
        """
    ).fetchall()

    results["top_customers"] = conn.execute(
        """
        SELECT c.name, COUNT(*) AS order_count
        FROM orders o JOIN customers c ON o.customer_id = c.id
        GROUP BY c.name ORDER BY order_count DESC LIMIT 5
        """
    ).fetchall()

    return results


def build_jaffle_shop(db_path: str, *, years: int = 1, force: bool = False) -> bool:
    """Generate the Jaffle Shop DuckDB at ``db_path`` if it does not already exist.

    Returns ``True`` if the DB was freshly generated, ``False`` if an existing
    file at ``db_path`` was reused. When ``force=True``, any existing file is
    overwritten. On the reuse path, dates are re-shifted so
    ``MAX(orders.ordered_at) == today`` — this keeps the demo feeling current
    even when the DB was built days or weeks earlier.
    """
    if os.path.exists(db_path) and not force:
        import duckdb

        conn = duckdb.connect(db_path)
        try:
            shift_dates_to_today(conn)
        finally:
            conn.close()
        return False

    check_dependencies()
    import duckdb

    if force and os.path.exists(db_path):
        os.remove(db_path)

    with tempfile.TemporaryDirectory(prefix="jaffle_") as tmpdir:
        data_dir = generate_data(output_dir=tmpdir, years=years)
        conn = duckdb.connect(db_path)
        try:
            create_schema(conn)
            load_data(conn=conn, data_dir=data_dir)
        finally:
            conn.close()
    return True


def ensure_demo_datasource(
    storage: StorageBackend,
    *,
    storage_path: str,
    name: str = DEMO_NAME,
    years: int = 1,
    ingest_models: bool = True,
    assume_yes: bool = True,
) -> Tuple[DatasourceConfig, List[SlayerModel], bool]:
    """Ensure the Jaffle Shop demo is fully set up in ``storage``.

    - Builds the DuckDB at ``<storage_base_dir>/demo/jaffle_shop.duckdb`` if missing.
    - Registers a datasource record (``name``, default ``jaffle_shop``).
    - Optionally auto-ingests models and sets ``default_time_dimension`` on
      ``orders``/``tweets``.

    Returns ``(datasource, jaffle_models, db_built)``. ``jaffle_models`` is
    the full set of Jaffle Shop ``SlayerModel`` objects currently in storage
    (whether ingested this call or from a previous run), so callers can report
    the true state of the demo. ``db_built`` is True when the DuckDB file was
    freshly generated this call.

    Raises ``DemoDependencyError`` when duckdb/jafgen is missing.
    """
    db_path = resolve_demo_db_path(storage_path)
    db_built = build_jaffle_shop(db_path=db_path, years=years)

    ds = DatasourceConfig.model_validate(
        {
            "name": name,
            "type": "duckdb",
            "database": db_path,
            "description": "Jaffle Shop demo (synthetic data via jafgen)",
        }
    )

    existing_ds = run_sync(storage.get_datasource(name))
    if existing_ds is None or assume_yes:
        run_sync(storage.save_datasource(ds))

    if not ingest_models:
        return ds, [], db_built

    # Fast path: DB was reused and models are already stored — return what's
    # on disk so callers can report the real count.
    existing_model_names = set(run_sync(storage.list_models()))
    if not db_built and all(t in existing_model_names for t in TABLE_NAMES):
        jaffle_models = [
            run_sync(storage.get_model(t)) for t in TABLE_NAMES if t in existing_model_names
        ]
        return ds, [m for m in jaffle_models if m is not None], db_built

    from slayer.engine.ingestion import ingest_datasource

    models = ingest_datasource(datasource=ds)
    written: List[SlayerModel] = []
    for model in models:
        if model.name in DEFAULT_TIME_DIMENSIONS:
            model.default_time_dimension = DEFAULT_TIME_DIMENSIONS[model.name]
        existing_model: Optional[SlayerModel] = run_sync(storage.get_model(model.name))
        if existing_model is not None and not assume_yes:
            written.append(existing_model)
            continue
        run_sync(storage.save_model(model))
        written.append(model)

    return ds, written, db_built
