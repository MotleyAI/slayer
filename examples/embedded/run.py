"""Embedded SLayer example — SQLite, no server needed.

Creates a SQLite database, seeds it, auto-ingests models (with rollup joins),
and runs sample queries.

Usage:
    cd examples/embedded
    python run.py
"""

import os
import sys
import tempfile

# Add project root to path for local development
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from seed import seed

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


def main():
    # Set up a temp directory for everything
    workdir = tempfile.mkdtemp(prefix="slayer_embedded_")
    db_path = os.path.join(workdir, "demo.db")
    conn_str = f"sqlite:///{db_path}"

    print(f"Working directory: {workdir}\n")

    # 1. Seed the database
    print("=== Seeding database ===")
    seed(conn_str)

    # 2. Configure datasource and storage
    storage = YAMLStorage(base_dir=os.path.join(workdir, "slayer_data"))
    ds = DatasourceConfig(name="demo", type="sqlite", database=db_path)
    run_sync(storage.save_datasource(ds))

    # 3. Auto-ingest models (with rollup joins) + set default time dimension
    print("\n=== Ingesting models ===")
    models = ingest_datasource(datasource=ds)
    for model in models:
        if model.name == "orders":
            model.default_time_dimension = "created_at"
        run_sync(storage.save_model(model))
        has_rollup = " (with rollup)" if model.sql else ""
        print(f"  {model.name}: {len(model.columns)} columns, {len(model.measures)} named formulas{has_rollup}")

    # 4. Run queries
    engine = SlayerQueryEngine(storage=storage)

    print("\n=== Query 1: Order count by status ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        measures=["*:count"],
        dimensions=["status"],
    ))
    for row in result.data:
        print(f"  {row['orders.status']}: {row['orders._count']}")

    print("\n=== Query 2: Revenue by product category (rollup join) ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        measures=["*:count", "quantity:sum"],
        dimensions=["products.category"],
        order=[{"column": "quantity_sum", "direction": "desc"}],
    ))
    for row in result.data:
        print(f"  {row['orders.products.category']}: {row['orders._count']} orders, {row['orders.quantity_sum']} units")

    print("\n=== Query 3: Orders by customer region (transitive rollup) ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        measures=["*:count"],
        dimensions=["customers.regions.name"],
    ))
    for row in result.data:
        print(f"  {row['orders.customers.regions.name']}: {row['orders._count']}")

    print("\n=== Query 4: Completed orders only (filter) ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        measures=["*:count", "quantity:sum"],
        filters=["status = 'completed'"],
    ))
    row = result.data[0]
    print(f"  Completed: {row['orders._count']} orders, {row['orders.quantity_sum']} units")

    print("\n=== Query 5: Top 3 customers by order count (rollup + order + limit) ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        measures=["*:count"],
        dimensions=["customers.name"],
        order=[{"column": "count", "direction": "desc"}],
        limit=3,
    ))
    for row in result.data:
        print(f"  {row['orders.customers.name']}: {row['orders._count']}")

    print("\n=== Query 6: Monthly orders with average quantity ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
        measures=[
            "*:count",
            "quantity:sum",
            {"formula": "quantity:sum / *:count", "name": "avg_qty"},
        ],
        order=[{"column": "created_at", "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        print(f"  {month}: {row['orders._count']} orders, avg qty {row['orders.avg_qty']:.1f}")

    print("\n=== Query 7: Monthly orders with cumulative sum ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
        measures=[
            "*:count",
            {"formula": "cumsum(*:count)", "name": "cumulative"},
        ],
        order=[{"column": "created_at", "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        print(f"  {month}: {row['orders._count']} orders, cumulative: {row['orders.cumulative']}")

    print("\n=== Query 8: Monthly orders with month-over-month change ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
        measures=[
            "*:count",
            {"formula": "time_shift(*:count, -1)", "name": "prev_month"},
            {"formula": "change(*:count)", "name": "mom_change"},
        ],
        order=[{"column": "created_at", "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        prev = row["orders.prev_month"] if row["orders.prev_month"] is not None else "-"
        chg = row["orders.mom_change"] if row["orders.mom_change"] is not None else "-"
        print(f"  {month}: {row['orders._count']} orders (prev: {prev}, change: {chg})")

    print("\n=== Query 9: Customer ranking by order count ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        dimensions=["customers.name"],
        measures=["*:count", {"formula": "rank(*:count)", "name": "rk"}],
        order=[{"column": "count", "direction": "desc"}],
    ))
    for row in result.data:
        print(f"  #{int(row['orders.rk'])} {row['orders.customers.name']}: {row['orders._count']} orders")

    print("\n=== Query 10: Aggregations + arithmetic in one measures list ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        dimensions=["products.category"],
        measures=[
            "*:count",
            "quantity:sum",
            {"formula": "quantity:sum / *:count", "name": "avg_qty"},
        ],
        order=[{"column": "count", "direction": "desc"}],
    ))
    for row in result.data:
        print(f"  {row['orders.products.category']}: {row['orders._count']} orders, avg qty {row['orders.avg_qty']:.1f}")

    print("\n=== Query 11: cumsum + change as transforms ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
        measures=[
            "*:count",
            {"formula": "cumsum(*:count)", "name": "running_total"},
            {"formula": "change(*:count)", "name": "mom_change"},
        ],
        order=[{"column": "created_at", "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        chg = row["orders.mom_change"] if row["orders.mom_change"] is not None else "-"
        print(f"  {month}: {row['orders._count']} orders, running: {row['orders.running_total']}, MoM: {chg}")

    print("\n=== Query 12: last() — most recent month's value broadcast ===")
    result = engine.execute_sync(query=SlayerQuery(
        source_model="orders",
        time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
        measures=[
            "*:count",
            {"formula": "last(*:count)", "name": "latest_month"},
        ],
        order=[{"column": "created_at", "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        print(f"  {month}: {row['orders._count']} orders (latest month: {row['orders.latest_month']})")

    print(f"\nDone! Database at: {db_path}")


if __name__ == "__main__":
    main()
