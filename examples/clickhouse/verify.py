"""Verification script for the ClickHouse Docker example.

Run after `docker compose up -d`:
    python examples/clickhouse/verify.py

ClickHouse has no FK constraints, so no rollup joins are generated.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from verify_common import (
    run_common_checks,
    check_rollup,
    check_median_percentile,
    check,
    check_column_types,
    summary,
)

if __name__ == "__main__":
    models = run_common_checks()
    check("4 models (no rollup)", len(models) == 4)
    check_rollup(expect_rollup=False)
    # Regression for issue #62 — ClickHouse Int32 / Float64 / DateTime must
    # round-trip as the right DataType, not as STRING. Note: DataType.TIMESTAMP
    # serialises as the string "time" (see slayer/core/enums.py).
    check_column_types(
        model_name="orders",
        expected_types={
            "id": "number",
            "customer_id": "number",
            "product_id": "number",
            "quantity": "number",
            "status": "string",
            "created_at": "time",
        },
    )
    check_column_types(
        model_name="customers",
        expected_types={
            "id": "number",
            "name": "string",
            "email": "string",
            "region_id": "number",
        },
    )
    check_column_types(
        model_name="products",
        expected_types={
            "id": "number",
            "name": "string",
            "category": "string",
            "price": "number",
        },
    )
    check_column_types(
        model_name="regions",
        expected_types={
            "id": "number",
            "name": "string",
        },
    )
    # Exercises the parametric quantile(p)(x) syntax SLayer emits for ClickHouse.
    check_median_percentile()
    summary()
