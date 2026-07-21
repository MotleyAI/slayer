"""Jaffle Shop demo dataset — bundled, one-command setup.

Generates ~2 years of synthetic coffee-shop data via ``jafgen``, loads it into
a DuckDB file under the storage directory, registers a ``jaffle_shop``
datasource, and (optionally) auto-ingests SLayer models enriched with curated
labels, descriptions, formats, and example measures (``DEMO_ENRICHMENT``). The default is kept
small so ``slayer serve --demo`` / ``slayer mcp --demo`` finish quickly enough
to fit inside MCP-client startup timeouts; bump ``--years`` for a richer
dataset (only the first four jafgen stores open within the first 2 years).

All operations are idempotent: re-running with the same storage path reuses
the existing DuckDB file instead of regenerating.
"""

import datetime as dt
import io
import os
import shutil
import subprocess
import sys
import tempfile
from collections import deque
from importlib.util import find_spec
from typing import IO, TYPE_CHECKING

from pydantic import BaseModel, Field

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    DatasourceConfig,
    ModelMeasure,
    SlayerModel,
)
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

LOAD_ORDER = ["customers", "stores", "products", "orders", "items", "supplies", "tweets"]

# CSV file stem (jafgen's ``raw_<name>.csv``) → DuckDB table name. Names match
# 1:1 today, but the mapping keeps the seam open if jafgen renames a file.
TABLE_NAMES = {name: name for name in LOAD_ORDER}

# Tables whose CSV column names diverge from our schema. Maps CSV name → SQL
# column name. Loader uses an explicit column-list INSERT for these tables so
# the rename is applied without depending on column ordering.
COLUMN_RENAMES = {
    "orders": {"customer": "customer_id"},
}

# Date columns per table — used to shift jafgen's hard-coded 2018-09-01 epoch
# forward so the demo data always ends near "today".
DATE_COLUMNS = {
    "stores": ["opened_at"],
    "orders": ["ordered_at"],
    "tweets": ["tweeted_at"],
}

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

CREATE TABLE items (
    id VARCHAR PRIMARY KEY,
    order_id VARCHAR NOT NULL REFERENCES orders(id),
    sku VARCHAR NOT NULL REFERENCES products(sku)
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


# --- curated semantic enrichment --------------------------------------------
#
# Auto-ingestion produces bare models: columns with types but no labels,
# descriptions, or measures. The jaffle schema is fixed and known, so a
# hand-curated layer of labels, descriptions, formats, example measures, and a
# custom-aggregation example is applied on top. All monetary columns are in
# dollars (the loader converts jafgen's cents on insert — see CENTS_COLUMNS).


def _currency() -> NumberFormat:
    return NumberFormat(type=NumberFormatType.CURRENCY, symbol="$", precision=2)


def _percent() -> NumberFormat:
    return NumberFormat(type=NumberFormatType.PERCENT, precision=1)


class _ColumnEnrichment(BaseModel):
    label: str
    description: str | None = None
    format: NumberFormat | None = None


class _TableEnrichment(BaseModel):
    description: str | None = None
    columns: dict[str, _ColumnEnrichment] = Field(default_factory=dict)
    measures: list[ModelMeasure] = Field(default_factory=list)
    aggregations: list[Aggregation] = Field(default_factory=list)


def _build_demo_enrichment() -> dict[str, _TableEnrichment]:
    return {
        "orders": _TableEnrichment(
            description=(
                "Customer orders — one row per order. Monetary amounts are in "
                "dollars. The fact table at the center of the demo: joins to "
                "customers and stores, and is referenced by items."
            ),
            # A built-in override: default the ``weighted_avg`` weight to
            # ``subtotal`` so a bare ``order_total:weighted_avg`` yields a
            # sales-weighted average order value.
            aggregations=[
                Aggregation(
                    name="weighted_avg",
                    params=[AggregationParam(name="weight", sql="subtotal")],
                    description="Weighted average defaulting the weight to order subtotal.",
                ),
            ],
            columns={
                "id": _ColumnEnrichment(label="Order ID"),
                "ordered_at": _ColumnEnrichment(
                    label="Order Date", description="When the order was placed."
                ),
                "store_id": _ColumnEnrichment(
                    label="Store ID", description="Store where the order was placed."
                ),
                "customer_id": _ColumnEnrichment(
                    label="Customer ID", description="Customer who placed the order."
                ),
                "subtotal": _ColumnEnrichment(
                    label="Net Sales",
                    description="Pre-tax order amount, in dollars.",
                    format=_currency(),
                ),
                "tax_paid": _ColumnEnrichment(
                    label="Tax Paid",
                    description="Tax charged on the order, in dollars.",
                    format=_currency(),
                ),
                "order_total": _ColumnEnrichment(
                    label="Order Total",
                    description="Final order amount including tax, in dollars.",
                    format=_currency(),
                ),
            },
            measures=[
                ModelMeasure(
                    name="total_revenue",
                    formula="order_total:sum",
                    label="Total Revenue",
                    description="Gross sales including tax, in dollars.",
                    type=DataType.DOUBLE,
                ),
                ModelMeasure(
                    name="net_sales",
                    formula="subtotal:sum",
                    label="Net Sales (pre-tax)",
                    description="Sales before tax, in dollars.",
                    type=DataType.DOUBLE,
                ),
                ModelMeasure(
                    name="tax_collected",
                    formula="tax_paid:sum",
                    label="Tax Collected",
                    description="Total tax collected, in dollars.",
                    type=DataType.DOUBLE,
                ),
                ModelMeasure(
                    name="order_count",
                    formula="id:count",
                    label="Orders",
                    description="Number of orders.",
                    type=DataType.INT,
                ),
                ModelMeasure(
                    name="unique_customers",
                    formula="customer_id:count_distinct",
                    label="Unique Customers",
                    description="Distinct customers who ordered.",
                    type=DataType.INT,
                ),
                ModelMeasure(
                    name="avg_order_value",
                    formula="order_total:sum / nullif(id:count, 0)",
                    label="Average Order Value",
                    description="Revenue per order, in dollars.",
                    type=DataType.DOUBLE,
                ),
                ModelMeasure(
                    name="effective_tax_rate",
                    formula="tax_paid:sum / nullif(subtotal:sum, 0)",
                    label="Effective Tax Rate",
                    description="Tax collected as a share of net sales.",
                    type=DataType.DOUBLE,
                ),
                ModelMeasure(
                    name="sales_weighted_aov",
                    formula="order_total:weighted_avg",
                    label="Sales-Weighted Avg Order",
                    description=(
                        "Average order total weighted by subtotal (larger orders "
                        "weigh more), in dollars."
                    ),
                    type=DataType.DOUBLE,
                ),
            ],
        ),
        "customers": _TableEnrichment(
            description="Customers of the Jaffle Shop — one row per person.",
            columns={
                "id": _ColumnEnrichment(label="Customer ID"),
                "name": _ColumnEnrichment(label="Customer", description="Customer name."),
            },
            measures=[
                ModelMeasure(
                    name="customer_count",
                    formula="id:count_distinct",
                    label="Customers",
                    description="Number of distinct customers.",
                    type=DataType.INT,
                ),
            ],
        ),
        "stores": _TableEnrichment(
            description="Physical Jaffle Shop locations.",
            columns={
                "id": _ColumnEnrichment(label="Store ID"),
                "name": _ColumnEnrichment(label="Store", description="Store name."),
                "opened_at": _ColumnEnrichment(
                    label="Opened", description="When the store opened."
                ),
                "tax_rate": _ColumnEnrichment(
                    label="Tax Rate",
                    description="Local sales-tax rate.",
                    format=_percent(),
                ),
            },
            measures=[
                ModelMeasure(
                    name="store_count",
                    formula="id:count_distinct",
                    label="Stores",
                    description="Number of stores.",
                    type=DataType.INT,
                ),
            ],
        ),
        "products": _TableEnrichment(
            description="Menu items sold at the Jaffle Shop — jaffles and beverages.",
            columns={
                "sku": _ColumnEnrichment(label="SKU", description="Product identifier."),
                "name": _ColumnEnrichment(label="Product", description="Product name."),
                "type": _ColumnEnrichment(
                    label="Category", description="Product category (jaffle or beverage)."
                ),
                "price": _ColumnEnrichment(
                    label="Price",
                    description="List price, in dollars.",
                    format=_currency(),
                ),
                "description": _ColumnEnrichment(
                    label="Description", description="Product description."
                ),
            },
            measures=[
                ModelMeasure(
                    name="product_count",
                    formula="sku:count_distinct",
                    label="Products",
                    description="Number of distinct products.",
                    type=DataType.INT,
                ),
                ModelMeasure(
                    name="avg_price",
                    formula="price:avg",
                    label="Average Price",
                    description="Average list price, in dollars.",
                    type=DataType.DOUBLE,
                ),
            ],
        ),
        "items": _TableEnrichment(
            description="Order line items — one row per unit sold on an order.",
            columns={
                "id": _ColumnEnrichment(label="Item ID"),
                "order_id": _ColumnEnrichment(
                    label="Order", description="Order this line belongs to."
                ),
                "sku": _ColumnEnrichment(
                    label="Product", description="Product sold on this line."
                ),
            },
            measures=[
                ModelMeasure(
                    name="units_sold",
                    formula="id:count",
                    label="Units Sold",
                    description="Number of item units sold.",
                    type=DataType.INT,
                ),
            ],
        ),
        "supplies": _TableEnrichment(
            description=(
                "Supplies (ingredients and packaging) used per product, with "
                "unit costs in dollars."
            ),
            columns={
                "id": _ColumnEnrichment(label="Supply ID"),
                "name": _ColumnEnrichment(label="Supply", description="Supply name."),
                "cost": _ColumnEnrichment(
                    label="Unit Cost",
                    description="Cost per unit, in dollars.",
                    format=_currency(),
                ),
                "perishable": _ColumnEnrichment(
                    label="Perishable", description="Whether the supply is perishable."
                ),
                "sku": _ColumnEnrichment(
                    label="Product", description="Product this supply is used for."
                ),
            },
            measures=[
                ModelMeasure(
                    name="total_supply_cost",
                    formula="cost:sum",
                    label="Total Supply Cost",
                    description="Total supply cost, in dollars.",
                    type=DataType.DOUBLE,
                ),
                ModelMeasure(
                    name="avg_unit_cost",
                    formula="cost:avg",
                    label="Avg Unit Cost",
                    description="Average supply unit cost, in dollars.",
                    type=DataType.DOUBLE,
                ),
            ],
        ),
        "tweets": _TableEnrichment(
            description="Synthetic customer tweets mentioning the Jaffle Shop.",
            columns={
                "id": _ColumnEnrichment(label="Tweet ID"),
                "user_id": _ColumnEnrichment(
                    label="Customer ID", description="Customer who tweeted."
                ),
                "tweeted_at": _ColumnEnrichment(
                    label="Tweet Date", description="When the tweet was posted."
                ),
                "content": _ColumnEnrichment(label="Tweet", description="Tweet text."),
            },
            measures=[
                ModelMeasure(
                    name="tweet_count",
                    formula="id:count",
                    label="Tweets",
                    description="Number of tweets.",
                    type=DataType.INT,
                ),
            ],
        ),
    }


DEMO_ENRICHMENT = _build_demo_enrichment()


def _is_auto_default_format(fmt: NumberFormat | None) -> bool:
    """True when ``fmt`` is unset or the bare ``NumberFormat(INTEGER/FLOAT)``
    default that auto-ingestion stamps on numeric columns — i.e. carries no
    user-supplied information and is safe to override."""
    if fmt is None:
        return True
    if fmt.type not in (NumberFormatType.INTEGER, NumberFormatType.FLOAT):
        return False
    return fmt.precision is None and fmt.symbol is None


def _enrich_column(column: Column, col_spec: _ColumnEnrichment) -> bool:
    """Fill unset label / description / auto-default format on one column."""
    changed = False
    if column.label is None:
        column.label = col_spec.label
        changed = True
    if column.description is None and col_spec.description is not None:
        column.description = col_spec.description
        changed = True
    if (
        col_spec.format is not None
        and column.format != col_spec.format
        and _is_auto_default_format(column.format)
    ):
        column.format = col_spec.format.model_copy(deep=True)
        changed = True
    return changed


def _new_named_entries(existing: list, additions: list) -> list:
    """Deep-copied ``additions`` whose ``name`` isn't already in ``existing``.

    Copies keep the module-level ``DEMO_ENRICHMENT`` spec isolated from any
    later in-place mutation of the attached model objects.
    """
    taken = {item.name for item in existing if item.name}
    return [item.model_copy(deep=True) for item in additions if item.name not in taken]


def apply_demo_enrichment(model: SlayerModel) -> bool:
    """Layer the curated jaffle enrichment onto an ingested demo model.

    Additive and idempotent, following the idempotent-ingest convention
    (DEV-1356): labels / descriptions are only filled where unset, formats
    only replace the auto-ingested bare INTEGER/FLOAT defaults, and
    measures / aggregations already present by name are left untouched — so
    user edits survive and re-running is a no-op. Only ``sql_table``-backed
    models are enriched (same convention): the curated spec assumes the
    fixed jaffle_shop table schemas, so a model redefined as sql-mode or
    query-backed is skipped. Returns ``True`` if the model was modified
    (callers save only on change).
    """
    spec = DEMO_ENRICHMENT.get(model.name)
    if spec is None or model.sql_table is None:
        return False

    changed = False

    if model.description is None and spec.description is not None:
        model.description = spec.description
        changed = True

    if (
        model.default_time_dimension is None
        and model.name in DEFAULT_TIME_DIMENSIONS
    ):
        model.default_time_dimension = DEFAULT_TIME_DIMENSIONS[model.name]
        changed = True

    by_name: dict[str, Column] = {c.name: c for c in model.columns}
    for col_name, col_spec in spec.columns.items():
        column = by_name.get(col_name)
        if column is not None and _enrich_column(column, col_spec):
            changed = True

    new_measures = _new_named_entries(model.measures, spec.measures)
    if new_measures:
        model.measures = list(model.measures) + new_measures
        changed = True

    new_aggs = _new_named_entries(model.aggregations, spec.aggregations)
    if new_aggs:
        model.aggregations = list(model.aggregations) + new_aggs
        changed = True

    return changed


def resolve_demo_db_path(storage_path: str) -> str:
    """Return the path to the Jaffle Shop DuckDB file for a given storage path.

    Creates the parent ``demo/`` directory if missing.
    """
    demo_dir = os.path.join(storage_base_dir(storage_path), "demo")
    os.makedirs(demo_dir, exist_ok=True)
    return os.path.join(demo_dir, "jaffle_shop.duckdb")


def _stream_fileno(stream) -> int | None:
    """Return ``stream.fileno()`` if it points at a real file descriptor, else None.

    ipykernel / nbclient replace ``sys.stdout`` / ``sys.stderr`` with shim
    streams whose ``fileno()`` raises ``io.UnsupportedOperation``; same with
    ``io.StringIO``. ``subprocess.run(stdout=stream)`` walks ``fileno()``
    unconditionally, so we need to detect that up front and fall back to a
    pumped Popen.
    """
    try:
        return stream.fileno()
    except (AttributeError, io.UnsupportedOperation, ValueError, OSError):
        return None


def _jafgen_cmd(years: int) -> list[str]:
    """Build the jafgen invocation without relying on PATH exposure.

    ``uv tool install motley-slayer`` (and pipx) link only slayer's own entry
    points onto PATH; the ``jafgen`` console script of the dependency stays
    inside the tool venv's ``bin/``, so a bare ``jafgen`` subprocess fails
    with FileNotFoundError. Since jafgen is a core dependency, prefer running
    its CLI through the current interpreter; fall back to a PATH lookup for
    environments where the package somehow isn't importable.
    """
    years_arg = str(max(1, years))
    if find_spec("jafgen") is not None:
        return [sys.executable, "-c", "from jafgen.cli import app; app()", years_arg]
    exe = shutil.which("jafgen")
    if exe is not None:
        return [exe, years_arg]
    raise RuntimeError(
        "jafgen is required to generate the demo data but was not found. "
        "It ships with motley-slayer — reinstall the package, or run "
        "`pip install jafgen` in the environment running slayer."
    )


def generate_data(
    output_dir: str,
    years: int = 1,
    *,
    stream: IO[str] | None = None,
) -> str:
    """Run ``jafgen`` into ``output_dir``; return the path to the generated CSVs.

    jafgen prints Rich progress bars to its own stdout. To keep them visible
    (and to avoid corrupting stdio-based protocols like MCP), both jafgen's
    stdout and stderr are routed to ``stream`` (default: this process's
    stderr). When ``stream`` is a real OS file (e.g. a TTY), the child
    inherits it directly so Rich can animate the bars in place. When it is a
    shim without ``fileno()`` (Jupyter ``OutStream``, ``io.StringIO``, …), we
    pump the child's output line by line into ``stream`` instead.
    """
    cmd = _jafgen_cmd(years)
    out = stream if stream is not None else sys.stderr
    # Force the child into Python UTF-8 mode (PEP 540). jafgen's Rich progress
    # bars emit non-Latin-1 glyphs (e.g. the 🥪 emoji); on Windows the child's
    # default stdio encoding is the ANSI code page (cp1252), which can't encode
    # them, so jafgen would die with UnicodeEncodeError and exit 1. PYTHONUTF8=1
    # switches its stdio to UTF-8 regardless of the host code page.
    child_env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    if _stream_fileno(out) is not None:
        try:
            subprocess.run(
                args=cmd, cwd=output_dir, check=True, stdout=out, stderr=out, env=child_env
            )
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"jafgen failed with exit code {e.returncode}") from e
        return os.path.join(output_dir, "jaffle-data")

    # Decode the pipe as UTF-8 (matching the child's forced encoding); errors are
    # replaced so the pump loop never crashes on a stray byte.
    tail: deque = deque(maxlen=25)
    with subprocess.Popen(
        args=cmd,
        cwd=output_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=child_env,
    ) as proc:
        assert proc.stdout is not None
        for line in proc.stdout:
            tail.append(line)
            out.write(line)
            try:
                out.flush()
            except Exception:
                pass
        rc = proc.wait()
    if rc != 0:
        detail = "".join(tail).rstrip()
        suffix = f":\n{detail}" if detail else ""
        raise RuntimeError(f"jafgen failed with exit code {rc}{suffix}")
    return os.path.join(output_dir, "jaffle-data")


def create_schema(
    conn: "duckdb.DuckDBPyConnection",
    schema_path: str | None = None,
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
        renames = COLUMN_RENAMES.get(csv_name, {})
        conn.execute(
            f"CREATE TEMP VIEW _{table}_raw AS SELECT * FROM read_csv_auto('{quoted_csv_path}')"
        )
        try:
            csv_columns = [row[0] for row in conn.execute(f"DESCRIBE _{table}_raw").fetchall()]
            select_parts = []
            target_columns = []
            for col in csv_columns:
                target_col = renames.get(col, col)
                target_columns.append(target_col)
                if col in cents_cols:
                    select_parts.append(f"CAST({col} AS DOUBLE) / 100.0 AS {target_col}")
                else:
                    select_parts.append(f"{col} AS {target_col}")
            conn.execute(
                f"INSERT INTO {table} ({', '.join(target_columns)}) "
                f"SELECT {', '.join(select_parts)} FROM _{table}_raw"
            )
        finally:
            conn.execute(f"DROP VIEW _{table}_raw")

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
        "items->orders": "SELECT COUNT(*) FROM items WHERE order_id NOT IN (SELECT id FROM orders)",
        "items->products": "SELECT COUNT(*) FROM items WHERE sku NOT IN (SELECT sku FROM products)",
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


def build_jaffle_shop(
    db_path: str,
    *,
    years: int = 2,
    force: bool = False,
    stream: IO[str] | None = None,
) -> bool:
    """Generate the Jaffle Shop DuckDB at ``db_path`` if it does not already exist.

    Returns ``True`` if the DB was freshly generated, ``False`` if an existing
    file at ``db_path`` was reused. When ``force=True``, any existing file is
    overwritten. On the reuse path, dates are re-shifted so
    ``MAX(orders.ordered_at) == today`` — this keeps the demo feeling current
    even when the DB was built days or weeks earlier. ``stream`` is forwarded
    to ``generate_data`` so jafgen's Rich progress bars stay visible.
    """
    import duckdb

    if os.path.exists(db_path) and not force:
        conn = duckdb.connect(db_path)
        try:
            shift_dates_to_today(conn)
        finally:
            conn.close()
        return False

    if force and os.path.exists(db_path):
        os.remove(db_path)

    with tempfile.TemporaryDirectory(prefix="jaffle_") as tmpdir:
        data_dir = generate_data(output_dir=tmpdir, years=years, stream=stream)
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
    years: int = 2,
    ingest_models: bool = True,
    assume_yes: bool = True,
    stream: IO[str] | None = None,
) -> tuple[DatasourceConfig, list[SlayerModel], bool]:
    """Ensure the Jaffle Shop demo is fully set up in ``storage``.

    - Builds the DuckDB at ``<storage_base_dir>/demo/jaffle_shop.duckdb`` if missing.
    - Registers a datasource record (``name``, default ``jaffle_shop``).
    - Optionally auto-ingests models, sets ``default_time_dimension`` on
      ``orders``/``tweets``, and applies the curated semantic enrichment
      (labels, descriptions, formats, measures — see ``DEMO_ENRICHMENT``).

    Returns ``(datasource, jaffle_models, db_built)``. ``jaffle_models`` is
    the full set of Jaffle Shop ``SlayerModel`` objects currently in storage
    (whether ingested this call or from a previous run), so callers can report
    the true state of the demo. ``db_built`` is True when the DuckDB file was
    freshly generated this call.
    """
    db_path = resolve_demo_db_path(storage_path)
    db_built = build_jaffle_shop(db_path=db_path, years=years, stream=stream)

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
    # on disk so callers can report the real count. All lookups are scoped to
    # the demo datasource: bare-name calls raise on storages whose models span
    # multiple datasources (DEV-1330). The enrichment pass is additive-only,
    # so demos set up by older versions gain labels/measures on next startup
    # while user edits are preserved.
    existing_model_names = set(run_sync(storage.list_models(data_source=name)))
    if not db_built and all(t in existing_model_names for t in TABLE_NAMES):
        jaffle_models = []
        for t in TABLE_NAMES:
            if t not in existing_model_names:
                continue
            model = run_sync(storage.get_model(name=t, data_source=name))
            if model is None:
                continue
            if assume_yes and apply_demo_enrichment(model):
                run_sync(storage.save_model(model))
            jaffle_models.append(model)
        return ds, jaffle_models, db_built

    from slayer.engine.ingestion import ingest_datasource

    models = ingest_datasource(datasource=ds)
    written: list[SlayerModel] = []
    for model in models:
        apply_demo_enrichment(model)
        existing_model: SlayerModel | None = run_sync(
            storage.get_model(name=model.name, data_source=name)
        )
        if existing_model is not None and not assume_yes:
            written.append(existing_model)
            continue
        run_sync(storage.save_model(model))
        written.append(model)

    return ds, written, db_built
