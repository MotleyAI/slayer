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

from slayer.core.models import DatasourceConfig
from slayer.core.query import Field, SlayerQuery
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
    storage.save_datasource(ds)

    # 3. Auto-ingest models (with rollup joins) + set default time dimension
    print("\n=== Ingesting models ===")
    models = ingest_datasource(datasource=ds)
    for model in models:
        if model.name == "orders":
            model.default_time_dimension = "created_at"
        storage.save_model(model)
        has_rollup = " (with rollup)" if model.sql else ""
        print(f"  {model.name}: {len(model.dimensions)} dims, {len(model.measures)} measures{has_rollup}")

    # 4. Run queries
    engine = SlayerQueryEngine(storage=storage)

    print("\n=== Query 1: Order count by status ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        fields=[{"formula": "count"}],
        dimensions=[{"name": "status"}],
    ))
    for row in result.data:
        print(f"  {row['orders.status']}: {row['orders.count']}")

    print("\n=== Query 2: Revenue by product category (rollup join) ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        fields=[{"formula": "count"}, {"formula": "quantity_sum"}],
        dimensions=[{"name": "products__category"}],
        order=[{"column": {"name": "quantity_sum"}, "direction": "desc"}],
    ))
    for row in result.data:
        print(f"  {row['orders.products__category']}: {row['orders.count']} orders, {row['orders.quantity_sum']} units")

    print("\n=== Query 3: Orders by customer region (transitive rollup) ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        fields=[{"formula": "count"}],
        dimensions=[{"name": "regions__name"}],
    ))
    for row in result.data:
        print(f"  {row['orders.regions__name']}: {row['orders.count']}")

    print("\n=== Query 4: Completed orders only (filter) ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        fields=[{"formula": "count"}, {"formula": "quantity_sum"}],
        filters=["status == 'completed'"],
    ))
    row = result.data[0]
    print(f"  Completed: {row['orders.count']} orders, {row['orders.quantity_sum']} units")

    print("\n=== Query 5: Top 3 customers by order count (rollup + order + limit) ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        fields=[{"formula": "count"}],
        dimensions=[{"name": "customers__name"}],
        order=[{"column": {"name": "count"}, "direction": "desc"}],
        limit=3,
    ))
    for row in result.data:
        print(f"  {row['orders.customers__name']}: {row['orders.count']}")

    print("\n=== Query 6: Monthly orders with average quantity (field) ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[Field(formula="count"), Field(formula="quantity_sum"), Field(formula="quantity_sum / count", name="avg_qty")],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        print(f"  {month}: {row['orders.count']} orders, avg qty {row['orders.avg_qty']:.1f}")

    print("\n=== Query 7: Monthly orders with cumulative sum (field) ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[Field(formula="count"), Field(formula="cumsum(count)", name="cumulative")],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        print(f"  {month}: {row['orders.count']} orders, cumulative: {row['orders.cumulative']}")

    print("\n=== Query 8: Monthly orders with month-over-month change (field) ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[
            Field(formula="count"),
            Field(formula="lag(count)", name="prev_month"),
            Field(formula="change(count)", name="mom_change"),
        ],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        prev = row["orders.prev_month"] or "-"
        chg = row["orders.mom_change"] if row["orders.mom_change"] is not None else "-"
        print(f"  {month}: {row['orders.count']} orders (prev: {prev}, change: {chg})")

    print("\n=== Query 9: Customer ranking by order count (field) ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        dimensions=[{"name": "customers__name"}],
        fields=[Field(formula="count"), Field(formula="rank(count)", name="rank")],
        order=[{"column": {"name": "count"}, "direction": "desc"}],
    ))
    for row in result.data:
        print(f"  #{int(row['orders.rank'])} {row['orders.customers__name']}: {row['orders.count']} orders")

    # --- Unified Fields syntax (recommended) ---

    print("\n=== Query 10: Unified fields — measures + expression in one list ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        dimensions=[{"name": "products__category"}],
        fields=[
            Field(formula="count"),
            Field(formula="quantity_sum"),
            Field(formula="quantity_sum / count", name="avg_qty"),
        ],
        order=[{"column": {"name": "count"}, "direction": "desc"}],
    ))
    for row in result.data:
        print(f"  {row['orders.products__category']}: {row['orders.count']} orders, avg qty {row['orders.avg_qty']:.1f}")

    print("\n=== Query 11: Unified fields — cumsum + change as formulas ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[
            Field(formula="count"),
            Field(formula="cumsum(count)", name="running_total"),
            Field(formula="change(count)", name="mom_change"),
        ],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        chg = row["orders.mom_change"] if row["orders.mom_change"] is not None else "-"
        print(f"  {month}: {row['orders.count']} orders, running: {row['orders.running_total']}, MoM: {chg}")

    print("\n=== Query 12: Unified fields — last() most recent value ===")
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[
            Field(formula="count"),
            Field(formula="last(count)", name="latest_month"),
        ],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    for row in result.data:
        month = str(row["orders.created_at"])[:7]
        print(f"  {month}: {row['orders.count']} orders (latest month: {row['orders.latest_month']})")

    print(f"\nDone! Database at: {db_path}")


if __name__ == "__main__":
    main()
