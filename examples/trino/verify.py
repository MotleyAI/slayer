"""Verification script for the Trino Docker example.

Run after `docker compose up -d`:
    python examples/trino/verify.py

The memory connector has no FK constraints, so no rollup relationships are generated.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from verify_common import (  # ALLOW(import-not-top): sys.path bootstrap for the shared examples/verify_common.py
    COUNT_METRIC,
    ORDERS,
    ORDERS_COUNT,
    QUERY_PATH,
    TOTAL_ORDERS,
    api,
    check,
    check_field_types,
    check_corr_covar,
    check_rollup,
    check_stddev_var,
    run_common_checks,
    summary,
)


def check_cardinality_invariant():
    print("\nCardinality invariant (sum-of-grouped == total):")
    rows = api("POST", QUERY_PATH, {
        "population": "orders", "metrics": [COUNT_METRIC], "dimensions": ["status"],
    })["data"]
    summed = sum(r[ORDERS_COUNT] for r in rows)
    check(f"sum(orders by status) == total ({summed} == {TOTAL_ORDERS})", summed == TOTAL_ORDERS)


def check_approx_median_percentile():
    """``approx_percentile`` returns a seeded value, not an interpolated one."""
    print("\nMedian / percentile (approximate):")
    row = api("POST", QUERY_PATH, {
        "population": "orders",
        "metrics": ["median(quantity)", "percentile(quantity, p=0.25)", "percentile(quantity, p=0.75)"],
    })["data"][0]
    seeded = {o[3] for o in ORDERS}
    median = row["orders.quantity_median"]
    p25 = row["orders.quantity_percentile_p_0_25"]
    p75 = row["orders.quantity_percentile_p_0_75"]
    check(f"median, p25, p75 are seeded quantities ({median}, {p25}, {p75})", {median, p25, p75} <= seeded)
    check("p25 <= median <= p75", p25 <= median <= p75)


if __name__ == "__main__":
    datasets = run_common_checks()
    check("4 datasets (no rollup)", len(datasets) == 4)
    check_rollup(expect_rollup=False)
    check_field_types(
        dataset_name="orders",
        expected_types={
            "id": "Integer",
            "customer_id": "Integer",
            "product_id": "Integer",
            "quantity": "Integer",
            "status": "String",
            "created_at": "DateTime",
        },
    )
    check_field_types(
        dataset_name="customers",
        expected_types={"id": "Integer", "name": "String", "email": "String", "region_id": "Integer"},
    )
    check_field_types(
        dataset_name="products",
        expected_types={"id": "Integer", "name": "String", "category": "String", "price": "Decimal"},
    )
    check_field_types(dataset_name="regions", expected_types={"id": "Integer", "name": "String"})
    check_cardinality_invariant()
    check_approx_median_percentile()
    check_stddev_var()
    check_corr_covar()
    summary()
