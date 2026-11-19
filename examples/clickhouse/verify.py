"""Verification script for the ClickHouse Docker example.

Run after `docker compose up -d`:
    python examples/clickhouse/verify.py

ClickHouse has no FK constraints, so no rollup relationships are generated.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from verify_common import (  # ALLOW(import-not-top): sys.path bootstrap for the shared verify_common helper
    run_common_checks,
    check_rollup,
    check_median_percentile,
    check_stddev_var,
    check_corr_covar,
    check,
    check_field_types,
    summary,
)

if __name__ == "__main__":
    datasets = run_common_checks()
    check("4 datasets (no rollup)", len(datasets) == 4)
    check_rollup(expect_rollup=False)
    # ClickHouse Int32 / Float64 / DateTime must round-trip as their DataType, not as String.
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
        expected_types={
            "id": "Integer",
            "name": "String",
            "email": "String",
            "region_id": "Integer",
        },
    )
    check_field_types(
        dataset_name="products",
        expected_types={
            "id": "Integer",
            "name": "String",
            "category": "String",
            "price": "Float",
        },
    )
    check_field_types(
        dataset_name="regions",
        expected_types={
            "id": "Integer",
            "name": "String",
        },
    )
    # Exercises the parametric quantile(p)(x) syntax SLayer emits for ClickHouse.
    check_median_percentile()
    # ClickHouse has native stddev_*/var_*/corr/covar_* (sqlglot transpiles
    # var_samp -> varSamp and similar). DEV-1317 smoke.
    check_stddev_var()
    check_corr_covar()
    summary()
