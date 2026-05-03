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

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

# Repeated string literals hoisted to constants (Sonar python:S1192).
COUNT_MEASURE = "*:count"
COUNT_KEY = "orders._count"
CUMSUM_CHANGE_KEY = "orders.cumsum_change"
CHG_KEY = "orders.chg"
AVG_QTY_KEY = "orders.avg_qty"
CUMULATIVE_KEY = "orders.cumulative"

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
    run_sync(storage.save_datasource(ds))
    models = ingest_datasource(datasource=ds)
    for m in models:
        if m.name == "orders":
            m.default_time_dimension = "created_at"
        run_sync(storage.save_model(m))

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

    orders_model = run_sync(storage.get_model("orders"))
    check(name="orders model exists", condition=orders_model is not None)
    if orders_model is None:
        return
    check(name="orders has dynamic joins", condition=len(orders_model.joins) > 0 and orders_model.sql_table is not None)
    check(name="orders has default_time_dimension", condition=orders_model.default_time_dimension == "created_at")

    column_names = [c.name for c in orders_model.columns]
    check(name="orders has quantity column", condition="quantity" in column_names)
    join_targets = [j.target_model for j in orders_model.joins]
    check(name="orders joins to customers", condition="customers" in join_targets)
    check(name="orders joins to products", condition="products" in join_targets)

    regions_model = run_sync(storage.get_model("regions"))
    check(name="regions model exists", condition=regions_model is not None)
    if regions_model is not None:
        check(name="regions has no rollup (sql_table set)", condition=regions_model.sql_table is not None)

    # --- Basic query checks ---
    print("\nBasic queries:")

    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            measures=[COUNT_MEASURE],
        )
    )
    check(name=f"total orders = {TOTAL_ORDERS}", condition=result.data[0][COUNT_KEY] == TOTAL_ORDERS)

    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            measures=[COUNT_MEASURE],
            dimensions=["status"],
        )
    )
    by_status = {r["orders.status"]: r[COUNT_KEY] for r in result.data}
    for status, expected in STATUS_COUNTS.items():
        check(name=f"{status} orders = {expected}", condition=by_status.get(status) == expected)

    # Rollup: by product category
    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            measures=[COUNT_MEASURE],
            dimensions=["products.category"],
        )
    )
    by_cat = {r["orders.products.category"]: r[COUNT_KEY] for r in result.data}
    check(name="all categories sum to total", condition=sum(by_cat.values()) == TOTAL_ORDERS)

    # Filter
    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            measures=[COUNT_MEASURE],
            filters=["status = 'completed'"],
        )
    )
    check(name=f"filtered completed = {STATUS_COUNTS['completed']}", condition=result.data[0][COUNT_KEY] == STATUS_COUNTS["completed"])

    # Order + limit
    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            measures=[COUNT_MEASURE],
            dimensions=["customers.name"],
            order=[{"column": "count", "direction": "desc"}],
            limit=3,
        )
    )
    check(name="top 3 customers returned", condition=result.row_count == 3)

    # --- Arithmetic measures ---
    print("\nMeasures (arithmetic):")

    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[
                COUNT_MEASURE,
                "quantity:sum",
                {"formula": "quantity:sum / *:count", "name": "avg_qty"},
            ],
            order=[{"column": "created_at", "direction": "asc"}],
        )
    )
    check(name="arithmetic measure produces results", condition=result.row_count == 12)
    check(name="avg_qty column exists", condition=AVG_QTY_KEY in result.columns)
    all_positive = all(row[AVG_QTY_KEY] > 0 for row in result.data)
    check(name="avg_qty all positive", condition=all_positive)

    print("\nMeasures (transforms):")

    # Cumulative sum
    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[COUNT_MEASURE, {"formula": "cumsum(*:count)", "name": "cumulative"}],
            order=[{"column": "created_at", "direction": "asc"}],
        )
    )
    check(name="cumsum produces results", condition=result.row_count == 12)
    check(name="cumsum column exists", condition=CUMULATIVE_KEY in result.columns)
    check(name=f"cumsum final = {TOTAL_ORDERS}", condition=result.data[-1][CUMULATIVE_KEY] == TOTAL_ORDERS)
    cumvals = [r[CUMULATIVE_KEY] for r in result.data]
    check(name="cumsum non-decreasing", condition=all(a <= b for a, b in zip(cumvals, cumvals[1:])))

    # time_shift (row-based, previous period)
    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[COUNT_MEASURE, {"formula": "time_shift(*:count, -1)", "name": "prev"}],
            order=[{"column": "created_at", "direction": "asc"}],
        )
    )
    check(name="time_shift first month is null", condition=result.data[0]["orders.prev"] is None)
    check(name="time_shift second month = first month count", condition=result.data[1]["orders.prev"] == result.data[0][COUNT_KEY])

    # Change
    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[COUNT_MEASURE, {"formula": "change(*:count)", "name": "chg"}],
            order=[{"column": "created_at", "direction": "asc"}],
        )
    )
    check(name="change first month is null", condition=result.data[0][CHG_KEY] is None)
    expected_change = result.data[1][COUNT_KEY] - result.data[0][COUNT_KEY]
    check(name=f"change second month = {expected_change}", condition=result.data[1][CHG_KEY] == expected_change)

    # Rank
    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            dimensions=["customers.name"],
            measures=[COUNT_MEASURE, {"formula": "rank(*:count)", "name": "rnk"}],
            order=[{"column": "count", "direction": "desc"}],
        )
    )
    check(name="rank column exists", condition="orders.rnk" in result.columns)
    check(name="rank #1 is first row", condition=result.data[0]["orders.rnk"] == 1)

    # --- Combined measures ---
    print("\nMeasures (combined):")

    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            dimensions=["products.category"],
            measures=[
                COUNT_MEASURE,
                "quantity:sum",
                {"formula": "quantity:sum / *:count", "name": "avg_qty"},
            ],
        )
    )
    check(name="combined measures produce results", condition=result.row_count > 0)
    check(name="expression column exists", condition=AVG_QTY_KEY in result.columns)
    check(name="count column exists", condition=COUNT_KEY in result.columns)

    # cumsum + change in one query
    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[
                COUNT_MEASURE,
                {"formula": "cumsum(*:count)", "name": "running"},
                {"formula": "change(*:count)", "name": "chg"},
            ],
            order=[{"column": "created_at", "direction": "asc"}],
        )
    )
    check(name="cumsum + change produces 12 months", condition=result.row_count == 12)
    check(name="running column exists", condition="orders.running" in result.columns)
    check(name="chg column exists", condition=CHG_KEY in result.columns)
    check(name=f"cumsum final = {TOTAL_ORDERS}", condition=result.data[-1]["orders.running"] == TOTAL_ORDERS)

    # last() — broadcast latest value
    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[COUNT_MEASURE, {"formula": "last(*:count)", "name": "latest"}],
            order=[{"column": "created_at", "direction": "asc"}],
        )
    )
    check(name="last column exists", condition="orders.latest" in result.columns)
    latest_vals = [r["orders.latest"] for r in result.data]
    check(name="last() is constant across rows", condition=len(set(latest_vals)) == 1)
    check(name="last() equals last month count", condition=latest_vals[0] == result.data[-1][COUNT_KEY])

    # --- Nested transforms ---
    print("\nNested transforms:")

    # Mathematical identity: cumsum(change(x)) == x - x[0]
    # For monthly counts, cumsum of changes should equal count minus first month's count
    result = engine.execute_sync(
        query=SlayerQuery(
            source_model="orders",
            time_dimensions=[{"dimension": "created_at", "granularity": "month"}],
            measures=[
                COUNT_MEASURE,
                {"formula": "cumsum(change(*:count))", "name": "cumsum_change"},
            ],
            order=[{"column": "created_at", "direction": "asc"}],
        )
    )
    check(name="nested cumsum(change()) works", condition=CUMSUM_CHANGE_KEY in result.columns)
    check(name="cumsum_change has 12 rows", condition=result.row_count == 12)
    # The first row's change is NULL, so cumsum_change[0] is NULL too.
    check(name="cumsum_change first row is null", condition=result.data[0][CUMSUM_CHANGE_KEY] is None)
    # Subsequent rows accumulate the per-period changes.
    non_null = [r[CUMSUM_CHANGE_KEY] for r in result.data[1:] if r[CUMSUM_CHANGE_KEY] is not None]
    check(name="cumsum_change has 11 non-null values from row 2", condition=len(non_null) == 11)

    # --- Summary ---
    print(f"\n{'=' * 40}")
    print(f"Results: {passed} passed, {failed} failed")
    if failed > 0:
        sys.exit(1)
    print("All checks passed!")


if __name__ == "__main__":
    main()
