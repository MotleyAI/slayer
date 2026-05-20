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
    check_stddev_var,
    check_corr_covar,
    check,
    check_column_types,
    summary,
)

if __name__ == "__main__":
    models = run_common_checks()
    check("4 models (no rollup)", len(models) == 4)
    check_rollup(expect_rollup=False)
    # Regression for issue #62 — ClickHouse Int32 / Float64 / DateTime must
    # round-trip as the right DataType, not as STRING. DataType vocabulary is
    # the sqlglot-aligned set from DEV-1361 (INT / DOUBLE / TEXT / TIMESTAMP /
    # DATE / BOOLEAN).
    check_column_types(
        model_name="orders",
        expected_types={
            "id": "INT",
            "customer_id": "INT",
            "product_id": "INT",
            "quantity": "INT",
            "status": "TEXT",
            "created_at": "TIMESTAMP",
        },
    )
    check_column_types(
        model_name="customers",
        expected_types={
            "id": "INT",
            "name": "TEXT",
            "email": "TEXT",
            "region_id": "INT",
        },
    )
    check_column_types(
        model_name="products",
        expected_types={
            "id": "INT",
            "name": "TEXT",
            "category": "TEXT",
            "price": "DOUBLE",
        },
    )
    check_column_types(
        model_name="regions",
        expected_types={
            "id": "INT",
            "name": "TEXT",
        },
    )
    # Exercises the parametric quantile(p)(x) syntax SLayer emits for ClickHouse.
    check_median_percentile()
    # ClickHouse has native stddev_*/var_*/corr/covar_* (sqlglot transpiles
    # var_samp -> varSamp and similar). DEV-1317 smoke.
    check_stddev_var()
    check_corr_covar()
    summary()
