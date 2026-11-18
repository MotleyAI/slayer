"""Verification script for the MySQL Docker example.

Run after `docker compose up -d`:
    python examples/mysql/verify.py

Note: MySQL may not expose inline FK constraints to SQLAlchemy inspector,
so rollup relationships may not be auto-generated.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from verify_common import (  # ALLOW(import-not-top): sys.path bootstrap for the shared verify_common helper
    api,
    run_common_checks,
    check_rollup,
    check_corr_covar,
    check_stddev_var,
    check,
    summary,
)

if __name__ == "__main__":
    datasets = run_common_checks()

    # MySQL may or may not have rollup depending on FK detection
    print("\nRollup:")
    orders_dataset = api("GET", "/datasets/orders")
    dim_names = [d["name"] for d in orders_dataset.get("dimensions", [])]
    has_rollup = any("__" in d for d in dim_names)
    if has_rollup:
        check_rollup(expect_rollup=True)
    else:
        print("  SKIP: no rollup (MySQL may not expose inline FK constraints to inspector)")
        check("4 datasets without rollup", len(datasets) == 4)

    # MySQL has native STDDEV_SAMP/STDDEV_POP/VAR_SAMP/VAR_POP.
    check_stddev_var()

    # MySQL corr/covar_samp/covar_pop now use a variance-decomposition formula.
    check_corr_covar()

    summary()
