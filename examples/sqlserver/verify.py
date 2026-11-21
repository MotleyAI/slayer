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
from verify_common import (  # ALLOW(import-not-top): sys.path bootstrap for the shared verify_common helper
    run_common_checks,
    check_rollup,
    check_stddev_var,
    check_corr_covar,
    check_field_types,
    summary,
)

if __name__ == "__main__":
    datasets = run_common_checks()
    check_rollup(expect_rollup=True)

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
        dataset_name="products",
        expected_types={
            "id": "Integer",
            "name": "String",
            "category": "String",
            "price": "Decimal",
        },
    )

    # T-SQL uses STDEV/STDEVP/VAR/VARP (not stddev_samp etc.) — verified via
    # the SQL generator; the API response is the same regardless of dialect.
    check_stddev_var()

    # corr/covar_samp/covar_pop via variance-decomposition formula.
    check_corr_covar()

    summary()
