"""Verification script for the embedded example — doubles as an integration test.

Usage:
    cd examples/embedded
    python verify.py
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from seed import seed, ORDERS

from slayer.core.models import DatasourceConfig
from slayer.core.query import Field, SlayerQuery
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

# Derive expected counts from seed data
TOTAL_ORDERS = len(ORDERS)
STATUS_COUNTS = {}
for o in ORDERS:
    STATUS_COUNTS[o[4]] = STATUS_COUNTS.get(o[4], 0) + 1


def main():
    workdir = tempfile.mkdtemp(prefix="slayer_verify_")
    db_path = os.path.join(workdir, "verify.db")
    conn_str = f"sqlite:///{db_path}"

    # Seed
    seed(conn_str)

    # Ingest
    storage = YAMLStorage(base_dir=os.path.join(workdir, "slayer_data"))
    ds = DatasourceConfig(name="demo", type="sqlite", database=db_path)
    storage.save_datasource(ds)
    models = ingest_datasource(datasource=ds)
    for m in models:
        if m.name == "orders":
            m.default_time_dimension = "created_at"
        storage.save_model(m)

    engine = SlayerQueryEngine(storage=storage)
    passed = 0
    failed = 0

    def check(name, condition):
        nonlocal passed, failed
        if condition:
            print(f"  PASS: {name}")
            passed += 1
        else:
            print(f"  FAIL: {name}")
            failed += 1

    # --- Model structure checks ---
    print("Model structure:")

    orders_model = storage.get_model("orders")
    check("orders model exists", orders_model is not None)
    check("orders has rollup SQL", orders_model.sql is not None and "LEFT JOIN" in orders_model.sql)
    check("orders has default_time_dimension", orders_model.default_time_dimension == "created_at")

    dim_names = [d.name for d in orders_model.dimensions]
    check("orders has customers__name rollup dim", "customers__name" in dim_names)
    check("orders has products__category rollup dim", "products__category" in dim_names)

    measure_names = [m.name for m in orders_model.measures]
    check("orders has quantity_sum measure", "quantity_sum" in measure_names)
    check("orders has customers__count measure", "customers__count" in measure_names)

    regions_model = storage.get_model("regions")
    check("regions has no rollup (sql_table set)", regions_model.sql_table is not None)

    # --- Basic query checks ---
    print("\nBasic queries:")

    result = engine.execute(query=SlayerQuery(
        model="orders", fields=[{"formula": "count"}],
    ))
    check(f"total orders = {TOTAL_ORDERS}", result.data[0]["orders.count"] == TOTAL_ORDERS)

    result = engine.execute(query=SlayerQuery(
        model="orders",
        fields=[{"formula": "count"}],
        dimensions=[{"name": "status"}],
    ))
    by_status = {r["orders.status"]: r["orders.count"] for r in result.data}
    for status, expected in STATUS_COUNTS.items():
        check(f"{status} orders = {expected}", by_status.get(status) == expected)

    # Rollup: by product category
    result = engine.execute(query=SlayerQuery(
        model="orders",
        fields=[{"formula": "count"}],
        dimensions=[{"name": "products__category"}],
    ))
    by_cat = {r["orders.products__category"]: r["orders.count"] for r in result.data}
    check("all categories sum to total", sum(by_cat.values()) == TOTAL_ORDERS)

    # Filter
    result = engine.execute(query=SlayerQuery(
        model="orders",
        fields=[{"formula": "count"}],
        filters=["status == 'completed'"],
    ))
    check(f"filtered completed = {STATUS_COUNTS['completed']}", result.data[0]["orders.count"] == STATUS_COUNTS["completed"])

    # Order + limit
    result = engine.execute(query=SlayerQuery(
        model="orders",
        fields=[{"formula": "count"}],
        dimensions=[{"name": "customers__name"}],
        order=[{"column": {"name": "count"}, "direction": "desc"}],
        limit=3,
    ))
    check("top 3 customers returned", result.row_count == 3)

    # --- Fields checks ---
    print("\nFields (arithmetic):")

    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[Field(formula="count"), Field(formula="quantity_sum"), Field(formula="quantity_sum / count", name="avg_qty")],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    check("arithmetic field produces results", result.row_count == 12)
    check("avg_qty column exists", "orders.avg_qty" in result.columns)
    all_positive = all(row["orders.avg_qty"] > 0 for row in result.data)
    check("avg_qty all positive", all_positive)

    print("\nFields (transforms):")

    # Cumulative sum
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[Field(formula="count"), Field(formula="cumsum(count)", name="cumulative")],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    check("cumsum produces results", result.row_count == 12)
    check("cumsum column exists", "orders.cumulative" in result.columns)
    check(f"cumsum final = {TOTAL_ORDERS}", result.data[-1]["orders.cumulative"] == TOTAL_ORDERS)
    cumvals = [r["orders.cumulative"] for r in result.data]
    check("cumsum non-decreasing", all(a <= b for a, b in zip(cumvals, cumvals[1:])))

    # Lag
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[Field(formula="count"), Field(formula="lag(count)", name="prev")],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    check("lag first month is null", result.data[0]["orders.prev"] is None)
    check("lag second month = first month count", result.data[1]["orders.prev"] == result.data[0]["orders.count"])

    # Change
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[Field(formula="count"), Field(formula="change(count)", name="chg")],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    check("change first month is null", result.data[0]["orders.chg"] is None)
    expected_change = result.data[1]["orders.count"] - result.data[0]["orders.count"]
    check(f"change second month = {expected_change}", result.data[1]["orders.chg"] == expected_change)

    # Rank
    result = engine.execute(query=SlayerQuery(
        model="orders",
        dimensions=[{"name": "customers__name"}],
        fields=[Field(formula="count"), Field(formula="rank(count)", name="rnk")],
        order=[{"column": {"name": "count"}, "direction": "desc"}],
    ))
    check("rank column exists", "orders.rnk" in result.columns)
    check("rank #1 is first row", result.data[0]["orders.rnk"] == 1)

    # --- Unified fields checks ---
    print("\nUnified fields:")

    # Fields: measure + expression
    result = engine.execute(query=SlayerQuery(
        model="orders",
        dimensions=[{"name": "products__category"}],
        fields=[
            Field(formula="count"),
            Field(formula="quantity_sum"),
            Field(formula="quantity_sum / count", name="avg_qty"),
        ],
    ))
    check("fields produce results", result.row_count > 0)
    check("field expression column exists", "orders.avg_qty" in result.columns)
    check("field measure column exists", "orders.count" in result.columns)

    # Fields: transform as formula
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[
            Field(formula="count"),
            Field(formula="cumsum(count)", name="running"),
            Field(formula="change(count)", name="chg"),
        ],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    check("fields cumsum produces 12 months", result.row_count == 12)
    check("fields cumsum column exists", "orders.running" in result.columns)
    check("fields change column exists", "orders.chg" in result.columns)
    check(f"fields cumsum final = {TOTAL_ORDERS}", result.data[-1]["orders.running"] == TOTAL_ORDERS)

    # Fields: last()
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[
            Field(formula="count"),
            Field(formula="last(count)", name="latest"),
        ],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    check("last column exists", "orders.latest" in result.columns)
    # last() should be the same value for every row (Dec count = 6)
    latest_vals = [r["orders.latest"] for r in result.data]
    check("last() is constant across rows", len(set(latest_vals)) == 1)
    check("last() equals last month count", latest_vals[0] == result.data[-1]["orders.count"])

    # --- Nested transforms ---
    print("\nNested transforms:")

    # Mathematical identity: cumsum(change(x)) == x - x[0]
    # For monthly counts, cumsum of changes should equal count minus first month's count
    result = engine.execute(query=SlayerQuery(
        model="orders",
        time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        fields=[
            Field(formula="count"),
            Field(formula="cumsum(change(count))", name="cumsum_change"),
        ],
        order=[{"column": {"name": "created_at"}, "direction": "asc"}],
    ))
    check("nested cumsum(change()) works", "orders.cumsum_change" in result.columns)
    # First row: cumsum of first change (NULL) = NULL
    # From row 2 onwards: cumsum(change(x)) = x - x[0]
    first_count = result.data[0]["orders.count"]
    for row in result.data[1:]:
        expected = row["orders.count"] - first_count
        if row["orders.cumsum_change"] != expected:
            check(f"cumsum(change(x)) == x - x[0] identity", False)
            break
    else:
        check("cumsum(change(x)) == x - x[0] identity", True)

    # --- Summary ---
    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed > 0:
        sys.exit(1)
    print("All checks passed!")


if __name__ == "__main__":
    main()
