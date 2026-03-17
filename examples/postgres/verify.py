"""Verification script for the Postgres Docker example.

Run after `docker compose up -d`:
    python examples/postgres/verify.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from verify_common import run_common_checks, check_rollup, summary

if __name__ == "__main__":
    run_common_checks()
    check_rollup(expect_rollup=True)
    summary()
