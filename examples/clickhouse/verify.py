"""Verification script for the ClickHouse Docker example.

Run after `docker compose up -d`:
    python examples/clickhouse/verify.py

ClickHouse has no FK constraints, so no rollup joins are generated.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from verify_common import run_common_checks, check_rollup, check, summary

if __name__ == "__main__":
    models = run_common_checks()
    check("4 models (no rollup)", len(models) == 4)
    check_rollup(expect_rollup=False)
    summary()
