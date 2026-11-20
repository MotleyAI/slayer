"""Embedded SLayer example — SQLite, no server needed.

Creates a SQLite database, seeds it, auto-ingests datasets (with rollup relationships),
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

from seed import seed  # ALLOW(import-not-top): sys.path bootstrap for the shared examples/seed.py

from slayer.async_utils import run_sync  # ALLOW(import-not-top): sys.path bootstrap for the shared examples/seed.py
from slayer.core.models import DatasourceConfig  # ALLOW(import-not-top): sys.path bootstrap for the shared examples/seed.py
from slayer.core.query import SlayerQuery  # ALLOW(import-not-top): sys.path bootstrap for the shared examples/seed.py
from slayer.engine.query_engine import SlayerQueryEngine  # ALLOW(import-not-top): sys.path bootstrap for the shared examples/seed.py
from slayer.storage.yaml_storage import YAMLStorage  # ALLOW(import-not-top): sys.path bootstrap for the shared examples/seed.py

# Repeated string literals hoisted to constants (Sonar python:S1192).
COUNT_METRIC = "count(orders.*)"
QUANTITY_SUM_METRIC = "sum(orders.quantity)"


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

    # 3. Auto-ingest datasets (with rollup relationships) + set default time dimension
    print("\n=== Ingesting datasets ===")
    engine = SlayerQueryEngine(storage=storage)
    datasets = engine.ingest_sync(ds)
    for dataset in datasets:
        if dataset.name == "orders":
            dataset.default_time_dimension = "created_at"
        run_sync(storage.save_dataset(dataset))
        has_rollup = " (with rollup)" if dataset.sql else ""
        print(f"  {dataset.name}: {len(dataset.fields)} fields, {len(dataset.metrics)} metrics{has_rollup}")

    # 4. Run queries

    print("\n=== Query 1: Order count by status ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        metrics=[COUNT_METRIC],
        dimensions=["orders.status"],
    ))
    for row in result.data:
        print(f"  {row['orders.status']}: {row['orders._count']}")

    print("\n=== Query 2: Revenue by product category (rollup relationship) ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        metrics=[COUNT_METRIC, QUANTITY_SUM_METRIC],
        dimensions=["products.category"],
        order=[{"field": QUANTITY_SUM_METRIC, "direction": "desc"}],
    ))
    for row in result.data:
        print(f"  {row['products.category']}: {row['orders._count']} orders, {row['orders.quantity_sum']} units")

    print("\n=== Query 3: Orders by customer region (transitive rollup) ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        metrics=[COUNT_METRIC],
        dimensions=["customers.regions.name"],
    ))
    for row in result.data:
        print(f"  {row['customers.regions.name']}: {row['orders._count']}")

    print("\n=== Query 4: Completed orders only (filter) ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        metrics=[COUNT_METRIC, QUANTITY_SUM_METRIC],
        filter=["orders.status = 'completed'"],
    ))
    row = result.data[0]
    print(f"  Completed: {row['orders._count']} orders, {row['orders.quantity_sum']} units")

    print("\n=== Query 5: Top 3 customers by order count (rollup + order + limit) ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        metrics=[COUNT_METRIC],
        dimensions=["customers.name"],
        order=[{"field": COUNT_METRIC, "direction": "desc"}],
        limit=3,
    ))
    for row in result.data:
        print(f"  {row['customers.name']}: {row['orders._count']}")

    print("\n=== Query 6: Monthly orders with average quantity ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        time_dimensions=[{"field": "orders.created_at", "granularity": "month"}],
        metrics=[
            COUNT_METRIC,
            QUANTITY_SUM_METRIC,
            {"metric": "sum(orders.quantity) / count(orders.*)", "alias": "avg_qty"},
        ],
        order=[{"field": "orders.created_at", "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        print(f"  {month}: {row['orders._count']} orders, avg qty {row['avg_qty']:.1f}")

    print("\n=== Query 7: Monthly orders with cumulative sum ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        time_dimensions=[{"field": "orders.created_at", "granularity": "month"}],
        metrics=[
            COUNT_METRIC,
            {"metric": "cumsum(count(orders.*))", "alias": "cumulative"},
        ],
        order=[{"field": "orders.created_at", "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        print(f"  {month}: {row['orders._count']} orders, cumulative: {row['cumulative']}")

    print("\n=== Query 8: Monthly orders with month-over-month change ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        time_dimensions=[{"field": "orders.created_at", "granularity": "month"}],
        metrics=[
            COUNT_METRIC,
            {"metric": "time_shift(count(orders.*), -1)", "alias": "prev_month"},
            {"metric": "change(count(orders.*))", "alias": "mom_change"},
        ],
        order=[{"field": "orders.created_at", "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        prev = row["prev_month"] if row["prev_month"] is not None else "-"
        chg = row["mom_change"] if row["mom_change"] is not None else "-"
        print(f"  {month}: {row['orders._count']} orders (prev: {prev}, change: {chg})")

    print("\n=== Query 9: Customer ranking by order count ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        dimensions=["customers.name"],
        metrics=[COUNT_METRIC, {"metric": "rank(count(orders.*), direction='desc')", "alias": "rk"}],
        order=[{"field": COUNT_METRIC, "direction": "desc"}],
    ))
    for row in result.data:
        print(f"  #{int(row['rk'])} {row['customers.name']}: {row['orders._count']} orders")

    print("\n=== Query 10: Aggregations + arithmetic in one metrics list ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        dimensions=["products.category"],
        metrics=[
            COUNT_METRIC,
            QUANTITY_SUM_METRIC,
            {"metric": "sum(orders.quantity) / count(orders.*)", "alias": "avg_qty"},
        ],
        order=[{"field": COUNT_METRIC, "direction": "desc"}],
    ))
    for row in result.data:
        print(f"  {row['products.category']}: {row['orders._count']} orders, avg qty {row['avg_qty']:.1f}")

    print("\n=== Query 11: cumsum + change as transforms ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        time_dimensions=[{"field": "orders.created_at", "granularity": "month"}],
        metrics=[
            COUNT_METRIC,
            {"metric": "cumsum(count(orders.*))", "alias": "running_total"},
            {"metric": "change(count(orders.*))", "alias": "mom_change"},
        ],
        order=[{"field": "orders.created_at", "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        chg = row["mom_change"] if row["mom_change"] is not None else "-"
        print(f"  {month}: {row['orders._count']} orders, running: {row['running_total']}, MoM: {chg}")

    print("\n=== Query 12: last() — most recent month's value broadcast ===")
    result = engine.execute_sync(query=SlayerQuery(
        population="orders",
        time_dimensions=[{"field": "orders.created_at", "granularity": "month"}],
        metrics=[
            COUNT_METRIC,
            {"metric": "last(count(orders.*))", "alias": "latest_month"},
        ],
        order=[{"field": "orders.created_at", "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        print(f"  {month}: {row['orders._count']} orders (latest month: {row['latest_month']})")

    print(f"\nDone! Database at: {db_path}")


if __name__ == "__main__":
    main()
