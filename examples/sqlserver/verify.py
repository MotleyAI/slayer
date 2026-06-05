"""Verification script for the SQL Server Docker example.

Run after `docker compose up -d`:
    python examples/sqlserver/verify.py

SQL Server 2022 supports STDEV/STDEVP/VAR/VARP natively; corr/covar_samp/
covar_pop use a variance-decomposition formula (no native function on T-SQL).
median/percentile are not supported on T-SQL and raise NotImplementedError.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from verify_common import (
    run_common_checks,
    check_rollup,
    check_stddev_var,
    check_corr_covar,
    check_column_types,
    summary,
)

if __name__ == "__main__":
    models = run_common_checks()
    check_rollup(expect_rollup=True)

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
        model_name="products",
        expected_types={
            "id": "INT",
            "name": "TEXT",
            "category": "TEXT",
            "price": "DOUBLE",
        },
    )

    # T-SQL uses STDEV/STDEVP/VAR/VARP (not stddev_samp etc.) — verified via
    # the SQL generator; the API response is the same regardless of dialect.
    check_stddev_var()

    # corr/covar_samp/covar_pop via variance-decomposition formula.
    check_corr_covar()

    summary()
