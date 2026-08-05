"""Verification script for the Redshift example.

Run after seeding + ingesting:
    python examples/seed.py "redshift+redshift_connector://user:pass@host:5439/dev"
    slayer datasources create "redshift+redshift_connector://user:pass@host:5439/dev" --name rs --ingest
    python examples/redshift/verify.py

Redshift's FOREIGN KEY constraints are declarative-only (not enforced), and
whether sqlalchemy-redshift's Inspector surfaces them for auto-ingestion join
discovery is unverified against a live cluster — treated here like
ClickHouse/BigQuery (no rollup joins expected), not like Postgres/MySQL/
Snowflake.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from verify_common import (
    check,
    check_column_types,
    check_corr_covar,
    check_median_percentile,
    check_rollup,
    check_stddev_var,
    run_common_checks,
    summary,
)

if __name__ == "__main__":
    models = run_common_checks()
    check("4 models (no rollup)", len(models) == 4)
    check_rollup(expect_rollup=False)
    # BIGINT -> INT; VARCHAR -> TEXT; DECIMAL(10,2) -> DOUBLE; TIMESTAMP -> TIMESTAMP.
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
    # RedshiftDialect (slayer/sql/dialects/_tier2.py) only overrides
    # build_approx_count_distinct — median/percentile/stddev/var/corr/covar
    # all fall through to the shared Postgres-shaped SqlDialect base, same as
    # every other Tier-2 dialect. Whether that's Redshift's *native* syntax
    # or a formula fallback is unconfirmed without a live cluster; this just
    # checks the queries execute and return sane values either way.
    check_median_percentile()
    check_stddev_var()
    check_corr_covar()
    summary()
