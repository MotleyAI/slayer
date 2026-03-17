"""Verification script for the MySQL Docker example.

Run after `docker compose up -d`:
    python examples/mysql/verify.py

Note: MySQL may not expose inline FK constraints to SQLAlchemy inspector,
so rollup joins may not be auto-generated.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from verify_common import run_common_checks, check_rollup, check, summary

if __name__ == "__main__":
    models = run_common_checks()

    # MySQL may or may not have rollup depending on FK detection
    print("\nRollup:")
    from verify_common import api
    orders_model = api("GET", "/models/orders")
    dim_names = [d["name"] for d in orders_model.get("dimensions", [])]
    has_rollup = any("__" in d for d in dim_names)
    if has_rollup:
        check_rollup(expect_rollup=True)
    else:
        print("  SKIP: no rollup (MySQL may not expose inline FK constraints to inspector)")
        check("4 models without rollup", len(models) == 4)

    summary()
