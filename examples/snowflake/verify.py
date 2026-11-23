"""Verification script for the Snowflake example (DEV-1551).

Run after seeding + ingesting:
    python examples/seed.py "snowflake://?connection_name=default"
    slayer datasources create "snowflake://?connection_name=default" --name sf --ingest
    python examples/snowflake/verify.py

Snowflake exposes declarative FK constraints via the Inspector, so the
rollup relationships ARE generated (unlike ClickHouse / BigQuery). Every aggregation
in the matrix is native — no formula fallbacks like MySQL / SQL Server.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from verify_common import (  # ALLOW(import-not-top): sys.path bootstrap for the shared verify_common helper
    check,
    check_field_types,
    check_corr_covar,
    check_median_percentile,
    check_rollup,
    check_stddev_var,
    run_common_checks,
    summary,
)

if __name__ == "__main__":
    datasets = run_common_checks()
    check("4 datasets", len(datasets) == 4)
    # Snowflake DOES expose declarative FK constraints — rollup relationships should
    # auto-discover. Behaviour matches Postgres / MySQL / SQLite, NOT
    # ClickHouse / BigQuery.
    check_rollup(expect_rollup=True)
    # NUMBER(38,0) → INT; NUMBER(10,2) → DOUBLE; TEXT → TEXT;
    # TIMESTAMP_NTZ → TIMESTAMP.
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
            "price": "Decimal",
        },
    )
    check_field_types(
        dataset_name="regions",
        expected_types={
            "id": "Integer",
            "name": "String",
        },
    )
    # Snowflake has native MEDIAN and PERCENTILE_CONT WITHIN GROUP.
    check_median_percentile()
    # Native STDDEV_SAMP / STDDEV_POP / VAR_SAMP / VAR_POP — no SqlDialect
    # variance-decomposition formula fallback.
    check_stddev_var()
    # Native CORR / COVAR_SAMP / COVAR_POP — no formula fallback.
    check_corr_covar()
    summary()
